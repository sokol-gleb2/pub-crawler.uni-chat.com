import "dotenv/config";
import axios from "axios";
import fs from "fs";
import csv from "csv-parser";
import { Parser } from "json2csv";

const GOOGLE_API_KEY = process.env.GOOGLE_API_KEY;

const GRID_FILE = "./files/edinburgh_1km_grid.csv";
const OUTPUT_FILE = "./files/edinburgh_pubs_full.csv";

const dedupeSet = new Set();
const results = [];

/* ------------------------------
   Helper: Sleep (avoid rate limit)
-------------------------------- */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/* ------------------------------
   Step 1: Nearby Search per grid
-------------------------------- */
async function searchNearby(lat, lon) {
    const response = await axios.post(
        "https://places.googleapis.com/v1/places:searchNearby",
        {
            includedTypes: ["bar"],
            maxResultCount: 20,
            locationRestriction: {
                circle: {
                    center: { latitude: lat, longitude: lon },
                    radius: 1000
                }
            }
        },
        {
            headers: {
                "Content-Type": "application/json",
                "X-Goog-Api-Key": GOOGLE_API_KEY,
                "X-Goog-FieldMask":
                    "places.id,places.displayName,places.photos"
            }
        }
    );

    return response.data.places || [];
}

/* ------------------------------
   Step 2: Fetch full details
-------------------------------- */
async function getPlaceDetails(placeId) {
    const response = await axios.get(
        `https://places.googleapis.com/v1/places/${placeId}`,
        {
            headers: {
                "X-Goog-Api-Key": GOOGLE_API_KEY,
                "X-Goog-FieldMask":
                    "displayName,websiteUri,location,regularOpeningHours,rating,editorialSummary,photos"
            }
        }
    );

    return response.data;
}

/* ------------------------------
   Step 3: Convert photo reference
-------------------------------- */
function getPhotoUrl(photoName) {
    return `https://places.googleapis.com/v1/${photoName}/media?maxHeightPx=800&key=${GOOGLE_API_KEY}`;
}

/* ------------------------------
   Main Execution
-------------------------------- */
async function run() {
    const gridPoints = [];

    // Load grid CSV
    await new Promise((resolve) => {
        fs.createReadStream(GRID_FILE)
            .pipe(csv())
            .on("data", (row) => {
                gridPoints.push({
                    latitude: parseFloat(row.latitude),
                    longitude: parseFloat(row.longitude)
                });
            })
            .on("end", resolve);
    });

    console.log(`Loaded ${gridPoints.length} grid cells`);

    for (const point of gridPoints) {
        console.log(`Searching grid: ${point.latitude}, ${point.longitude}`);

        try {
            const places = await searchNearby(point.latitude, point.longitude);

            for (const place of places) {
                if (dedupeSet.has(place.id)) continue;

                dedupeSet.add(place.id);

                await sleep(200); // prevent rate limit

                const details = await getPlaceDetails(place.id);

                const photos = details.photos
                    ? details.photos.slice(0, 2).map(p => getPhotoUrl(p.name))
                    : [];

                results.push({
                    name: details.displayName?.text || "",
                    website: details.websiteUri || "",
                    location: details.location
                        ? `${details.location.latitude}, ${details.location.longitude}`
                        : "",
                    opening_times: details.regularOpeningHours
                        ? details.regularOpeningHours.weekdayDescriptions?.join(" | ")
                        : "",
                    rating: details.rating || "",
                    summary: details.editorialSummary?.text || "",
                    photo_1: photos[0] || "",
                    photo_2: photos[1] || ""
                });

                console.log(`Added: ${details.displayName?.text}`);
            }

        } catch (err) {
            console.log("Error:", err.response?.data || err.message);
        }

        await sleep(300);
    }

    const parser = new Parser();
    const csvOutput = parser.parse(results);

    fs.writeFileSync(OUTPUT_FILE, csvOutput);

    console.log("Done. Saved to", OUTPUT_FILE);
}

run();