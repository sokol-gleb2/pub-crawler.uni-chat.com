import fs from "fs";
import csv from "csv-parser";
import { Parser } from "json2csv";

const INPUT_FILE = "./structured/pubs.csv";

const removeUrls = (str) => {
    return str.replace(/https?:\/\/[^\s]*|www\.[^\s]+/gi, '');
};

async function run() {
    const rows = [];

    await new Promise((resolve, reject) => {
        fs.createReadStream(INPUT_FILE)
            .pipe(csv())
            .on("data", (row) => {
                rows.push({
                    ...row,
                    description: removeUrls(row.description || ""),
                    student_discount: removeUrls(row.student_discount || "")
                });
            })
            .on("end", resolve)
            .on("error", reject);
    });

    const parser = new Parser();
    const csvOutput = parser.parse(rows);
    fs.writeFileSync(INPUT_FILE, csvOutput);

    console.log(`Done. Updated ${rows.length} rows in ${INPUT_FILE}`);
}

run().catch((error) => {
    console.error("Failed to clean CSV:", error.message);
    process.exit(1);
});
