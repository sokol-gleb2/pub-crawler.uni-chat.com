import "dotenv/config";
import fs from "fs";
import axios from "axios";
import csv from "csv-parser";
import { Parser } from "json2csv";

const FILES = [
    './complete/1_edinburgh_pubs_with_discounts.csv',
    './complete/2_edinburgh_pubs.csv',
    './complete/3_edinburgh_pubs_student_discounts.csv',
    './complete/4_edinburgh_pubs_with_student_discounts.csv',
    './complete/5_edinburgh_pubs_student_discounts.csv',
    './complete/6_report.csv',
    './complete/7_edinburgh_pubs_student_discounts.csv',
];
const OUTPUT_FILE = "./structured/pubs.csv";

const GOOGLE_API_KEY = process.env.GOOGLE_API_KEY;

async function getPlaceAddress(name, lat, lng) {
    try {
        const response = await axios.post(
            "https://places.googleapis.com/v1/places:searchText",
            {
                textQuery: name,
                locationBias: {
                    circle: {
                        center: {
                            latitude: lat,
                            longitude: lng
                        },
                        radius: 500 // meters
                    }
                }
            },
            {
                headers: {
                    "Content-Type": "application/json",
                    "X-Goog-Api-Key": GOOGLE_API_KEY,
                    "X-Goog-FieldMask": "places.displayName,places.formattedAddress"
                }
            }
        );

        if (!response.data.places || response.data.places.length === 0) {
            return null;
        }

        return response.data.places[0].formattedAddress;

    } catch (error) {
        console.error("Places error:", error.response?.data || error.message);
        return null;
    }
}

const isLatLon = (s) => {
    const m = s.trim().match(/^(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)$/);
    if (!m) return false;
    const lat = Number(m[1]), lon = Number(m[2]);
    return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
};


async function run() {
    const lines = [];
    for (const file of FILES) {
        // Load CSV
        await new Promise((resolve) => {
            fs.createReadStream(file)
                .pipe(csv())
                .on("data", (row) => {
                    lines.push({
                        name: row.name,
                        website: row.website,
                        location: row.location,
                        opening_times: row.opening_times,
                        rating: row.rating,
                        photo_1: row.photo_1,
                        photo_2: row.photo_2,
                        description: row.description,
                        student_discount_present: row.student_discount_present,
                        student_discount: row.student_discount
                    });
                })
                .on("end", resolve);
        });
    }

    console.log("Files loaded. " + lines.length + " pubs");
    
    for (const line of lines) {
        // Check if address is lang,lat:
        if (isLatLon(line.location)) {
            // Reverse geocoding with places API
            const address = await getPlaceAddress(line.name, parseFloat(line.location.split(',')[0]), parseFloat(line.location.split(',')[1]));
            line.location = address;
        }
    }

    const parser = new Parser();
    const csvOutput = parser.parse(lines);

    fs.writeFileSync(OUTPUT_FILE, csvOutput);

    console.log("Done. Saved to", OUTPUT_FILE);
}

run();