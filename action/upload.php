<?php

// session_start();
// if (!isset($_SESSION["username"]) || ($_SESSION["username"] !== "glebby")) {
//     header("Location: https://www.uni-chat.com/");
//     exit();
// }

require_once __DIR__ . "/../../server/PredisClient.php";
require_once __DIR__ . "/../../server/PostgresQueryHandler.php";
require_once __DIR__ . "/../../server/AWSClient.php";

header("Content-Type: application/json");

const CSV_PATH = __DIR__ . "/../structured/pubs.csv";
const S3_BUCKET = "media.uni-chat.co.uk";
const ROW_LIMIT = 5; // testing-only limit

// --- Helpers ---
function randomId13(): string {
    return uniqid();
}

function normalizeNullableString($value): ?string {
    if ($value === null) {
        return null;
    }

    $trimmed = trim((string)$value);
    return $trimmed === "" ? null : $trimmed;
}

function parseBoolean($value): bool {
    $v = strtolower(trim((string)$value));
    return in_array($v, ["1", "true", "t", "yes", "y"], true);
}

function parseRatingOrNull($value): ?string {
    $trimmed = trim((string)$value);
    if ($trimmed === "" || !is_numeric($trimmed)) {
        return null;
    }

    $rating = (float)$trimmed;
    if ($rating < 0 || $rating > 5) {
        return null;
    }

    return number_format($rating, 1, ".", "");
}

function parseLatLon(?string $value): array {
    if ($value === null) {
        return [null, null];
    }

    $parts = explode(",", $value);
    if (count($parts) !== 2) {
        return [null, null];
    }

    $lat = trim($parts[0]);
    $lon = trim($parts[1]);

    if (!is_numeric($lat) || !is_numeric($lon)) {
        return [null, null];
    }

    return [(float)$lat, (float)$lon];
}

function extFromUrl(string $url): string {
    $path = parse_url($url, PHP_URL_PATH);
    if (!is_string($path) || $path === "") {
        return "jpg";
    }

    $ext = strtolower(pathinfo($path, PATHINFO_EXTENSION));
    return $ext !== "" ? $ext : "jpg";
}

function getWritableTempDir(): ?string {
    $candidates = [
        sys_get_temp_dir(),
        "/tmp",
        __DIR__ . "/tmp_uploads",
        __DIR__ . "/../tmp_uploads"
    ];

    foreach ($candidates as $dir) {
        if (!is_string($dir) || $dir === "") {
            continue;
        }

        if (!is_dir($dir)) {
            @mkdir($dir, 0777, true);
            @chmod($dir, 0777);
        }

        if (!is_dir($dir) || !is_writable($dir)) {
            continue;
        }

        // Validate writability with a real file operation (more reliable than is_writable alone).
        $probe = @tempnam($dir, "probe_");
        if ($probe !== false) {
            @unlink($probe);
            return $dir;
        }
    }

    return null;
}

function downloadImageToTempWithError(string $url): array {
    $content = null;
    $error = null;

    // Prefer cURL for robust redirects and headers (Google image hosts can reject basic clients).
    if (function_exists("curl_init")) {
        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_FOLLOWLOCATION => true,
            CURLOPT_MAXREDIRS => 10,
            CURLOPT_CONNECTTIMEOUT => 10,
            CURLOPT_TIMEOUT => 30,
            CURLOPT_USERAGENT => "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            CURLOPT_HTTPHEADER => [
                "Accept: image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                "Referer: https://www.google.com/"
            ],
            CURLOPT_SSL_VERIFYPEER => true,
            CURLOPT_SSL_VERIFYHOST => 2
        ]);

        $curlBody = curl_exec($ch);
        $httpCode = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $curlErrNo = curl_errno($ch);
        $curlError = curl_error($ch);

        // Fallback for local/dev cert chain issues.
        if (($curlBody === false || $httpCode < 200 || $httpCode >= 300) && $curlErrNo !== 0) {
            curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
            curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
            $curlBody = curl_exec($ch);
            $httpCode = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
            $curlErrNo = curl_errno($ch);
            $curlError = curl_error($ch);
        }

        if ($curlBody !== false && $httpCode >= 200 && $httpCode < 300 && $curlBody !== "") {
            $content = $curlBody;
        } else {
            $error = "curl failed (http=" . $httpCode . ", errno=" . $curlErrNo . ", error=" . ($curlError !== "" ? $curlError : "none") . ")";
        }

        curl_close($ch);
    }

    // Fallback to streams if cURL unavailable or failed.
    if ($content === null) {
        $context = stream_context_create([
            "http" => [
                "method" => "GET",
                "timeout" => 25,
                "follow_location" => 1,
                "header" => "User-Agent: Mozilla/5.0\r\nReferer: https://www.google.com/\r\n"
            ],
            "https" => [
                "method" => "GET",
                "timeout" => 25,
                "follow_location" => 1,
                "header" => "User-Agent: Mozilla/5.0\r\nReferer: https://www.google.com/\r\n"
            ]
        ]);

        $streamBody = @file_get_contents($url, false, $context);
        $lastError = error_get_last();
        if ($streamBody !== false && $streamBody !== "") {
            $content = $streamBody;
        } elseif ($error === null) {
            $error = "stream failed" . (isset($lastError["message"]) ? ": " . $lastError["message"] : "");
        }
    }

    if ($content === null || $content === "") {
        return [null, $error ?? "download returned empty body"];
    }

    $tempDir = getWritableTempDir();
    if ($tempDir === null) {
        return [null, "no writable temp dir available"];
    }

    $tmpPath = tempnam($tempDir, "venue_img_");
    if ($tmpPath === false) {
        return [null, "tempnam failed in dir: " . $tempDir];
    }

    if (file_put_contents($tmpPath, $content) === false) {
        @unlink($tmpPath);
        return [null, "failed to write temporary file"];
    }

    return [$tmpPath, null];
}

function buildUploadPayload(array $photoUrls): array {
    $files = [
        "name" => [],
        "tmp_name" => [],
        "type" => [],
        "size" => [],
        "error" => []
    ];
    $meta = [];
    $tmpFiles = [];
    $errors = [];

    $photoKeyMap = [
        1 => "logo",
        2 => "cover"
    ];

    $photoIndex = 0;
    foreach ($photoUrls as $url) {
        $photoIndex++;
        if ($url === null) {
            continue;
        }

        [$tmpPath, $downloadError] = downloadImageToTempWithError($url);
        if ($tmpPath === null) {
            $errors[] = "photo_" . $photoIndex . " download failed: " . ($downloadError ?? "unknown reason");
            continue;
        }

        $ext = extFromUrl($url);
        $baseName = $photoKeyMap[$photoIndex] ?? ("photo_" . $photoIndex);
        $filename = $baseName . "." . $ext;

        $files["name"][] = $filename;
        $files["tmp_name"][] = $tmpPath;
        $files["type"][] = "";
        $files["size"][] = filesize($tmpPath) ?: 0;
        $files["error"][] = 0;

        $meta[] = [
            "id" => $baseName . "." . $ext,
            "type" => "image"
        ];

        $tmpFiles[] = $tmpPath;
    }

    return [$files, $meta, $tmpFiles, $errors];
}

function cleanupTmpFiles(array $tmpFiles): void {
    foreach ($tmpFiles as $tmpFile) {
        if (is_string($tmpFile) && $tmpFile !== "" && file_exists($tmpFile)) {
            @unlink($tmpFile);
        }
    }
}

function pickVenuePoints(bool $studentDiscountPresent, ?string $area): int {
    if (!$studentDiscountPresent) {
        return 0;
    }

    if ($area !== null && strcasecmp(trim($area), "Edinburgh") === 0) {
        return 0;
    }

    $totalWeight = 0;
    $cumulative = [];
    for ($points = 0; $points <= 50; $points++) {
        $weight = ($points === 10 || $points === 20) ? 5 : 1;
        $totalWeight += $weight;
        $cumulative[$points] = $totalWeight;
    }

    $roll = random_int(1, $totalWeight);
    foreach ($cumulative as $points => $threshold) {
        if ($roll <= $threshold) {
            return $points;
        }
    }

    return 0;
}

try {
    if (!file_exists(CSV_PATH)) {
        http_response_code(500);
        echo json_encode([
            "result" => "failure",
            "message" => "CSV file not found: " . CSV_PATH
        ]);
        exit();
    }

    $pg = new PostgresQueryHandler();
    $predis_client = (new PredisClient())->getClient();
    $aws = new AWSClient($predis_client);

    $handle = fopen(CSV_PATH, "r");
    if ($handle === false) {
        http_response_code(500);
        echo json_encode([
            "result" => "failure",
            "message" => "Unable to open CSV file"
        ]);
        exit();
    }

    $headers = fgetcsv($handle);
    if ($headers === false) {
        fclose($handle);
        http_response_code(500);
        echo json_encode([
            "result" => "failure",
            "message" => "CSV has no header row"
        ]);
        exit();
    }

    $processed = 0;
    $inserted = 0;
    $unsuccessful = [];
    $unsuccessfulDetails = [];
    $s3Checks = [];
    $dbFailed = [];

    $insertSql = "
        INSERT INTO venues (
            id, name, website, location, area, coordinates, opening_times, rating, description,
            student_discount_present, student_discount, points
        )
        VALUES (
            \$1, \$2, \$3, \$4, \$5,
            CASE
                WHEN \$6::double precision IS NULL OR \$7::double precision IS NULL THEN NULL
                ELSE ST_SetSRID(ST_MakePoint(\$7::double precision, \$6::double precision), 4326)::geography
            END,
            \$8, \$9, \$10, \$11, \$12, \$13
        )
    ";

    while (($row = fgetcsv($handle)) !== false) {
        if (count($row) === 1 && trim((string)$row[0]) === "") {
            continue;
        }

        if ($processed >= ROW_LIMIT) {
            break;
        }

        $data = [];
        foreach ($headers as $idx => $header) {
            $data[$header] = $row[$idx] ?? null;
        }

        $processed++;
        $venueId = randomId13();

        $name = trim((string)($data["name"] ?? ""));
        if ($name === "") {
            $dbFailed[] = [
                "id" => $venueId,
                "error" => "Missing name"
            ];
            continue;
        }

        $website = normalizeNullableString($data["website"] ?? null);
        $location = normalizeNullableString($data["location"] ?? null);
        $area = normalizeNullableString($data["area"] ?? null);
        [$lat, $lon] = parseLatLon(normalizeNullableString($data["langlat"] ?? null));
        $openingTimes = normalizeNullableString($data["opening_times"] ?? null);
        $rating = parseRatingOrNull($data["rating"] ?? null);
        $description = normalizeNullableString($data["description"] ?? null);
        $hasStudentDiscountPresent = parseBoolean($data["student_discount_present"] ?? "false");
        $studentDiscountPresent = $hasStudentDiscountPresent ? "true" : "false";
        $studentDiscount = normalizeNullableString($data["student_discount"] ?? null);
        $points = pickVenuePoints($hasStudentDiscountPresent, $area);

        $photo1 = normalizeNullableString($data["photo_1"] ?? null);
        $photo2 = normalizeNullableString($data["photo_2"] ?? null);

        $hadImageIssue = false;
        $imageErrors = [];

        [$files, $meta, $tmpFiles, $downloadErrors] = buildUploadPayload([$photo1, $photo2]);
        if (!empty($downloadErrors)) {
            $hadImageIssue = true;
            $imageErrors = array_merge($imageErrors, $downloadErrors);
        }

        if (count($files["name"]) > 0) {
            $folderName = "venues/" . $venueId;
            $uploadResult = $aws->uploadMediaToFolder($files, $meta, $folderName, S3_BUCKET);
            if (!isset($uploadResult["result"]) || $uploadResult["result"] !== "success") {
                $hadImageIssue = true;
                $uploadErrorMessage = $uploadResult["error"] ?? "Unknown S3 upload error";
                $uploadErrorFile = isset($uploadResult["file"]) ? (" for file " . $uploadResult["file"]) : "";
                $imageErrors[] = "S3 upload failed" . $uploadErrorFile . ": " . $uploadErrorMessage;
            } else {
                $folderMedia = $aws->getFolderMedia($folderName, S3_BUCKET);
                if (is_string($folderMedia)) {
                    $hadImageIssue = true;
                    $imageErrors[] = "S3 verification failed: " . $folderMedia;
                } elseif (is_array($folderMedia)) {
                    $actualCount = count($folderMedia);
                    $expectedCount = count($files["name"]);
                    $s3Checks[] = [
                        "id" => $venueId,
                        "bucket" => S3_BUCKET,
                        "folder" => $folderName,
                        "expected" => $expectedCount,
                        "actual" => $actualCount
                    ];
                    if ($actualCount < $expectedCount) {
                        $hadImageIssue = true;
                        $imageErrors[] = "S3 verification mismatch: expected " . $expectedCount . ", found " . $actualCount;
                    }
                }
            }
        }

        cleanupTmpFiles($tmpFiles);

        $dbResult = $pg->executeQuery($insertSql, [
            $venueId,
            $name,
            $website,
            $location,
            $area,
            $lat,
            $lon,
            $openingTimes,
            $rating,
            $description,
            $studentDiscountPresent,
            $studentDiscount,
            $points
        ]);

        if (($dbResult["result"] ?? "failure") === "failure") {
            $dbFailed[] = [
                "id" => $venueId,
                "error" => $dbResult["error"] ?? "Unknown DB error",
                "diag" => $dbResult["diag"] ?? null
            ];
            continue;
        }

        $inserted++;

        if ($hadImageIssue) {
            $unsuccessful[] = $venueId;
            $unsuccessfulDetails[] = [
                "id" => $venueId,
                "errors" => $imageErrors
            ];
        }
    }

    fclose($handle);

    echo json_encode([
        "result" => "success",
        "processed" => $processed,
        "inserted" => $inserted,
        "bucket" => S3_BUCKET,
        "unsuccessful" => $unsuccessful,
        "unsuccessful_details" => $unsuccessfulDetails,
        "s3_checks" => $s3Checks,
        "db_failed_count" => count($dbFailed),
        "db_failed" => $dbFailed
    ]);
} catch (Throwable $e) {
    http_response_code(500);
    echo json_encode([
        "result" => "failure",
        "message" => $e->getMessage()
    ]);
}