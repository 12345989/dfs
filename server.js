// Import the 'express' module to create the server.
const express = require('express');

// The 'path' module is a built-in Node.js module used for working with file and directory paths.
const path = require('path');

// Multer is middleware for handling multipart/form-data, which is primarily used for uploading files.
const multer = require('multer');

// The fs and util modules are needed for file system operations.
const fs = require('fs');
const util = require('util');
const stream = require('stream');

// Promisify fs.unlink to use it with async/await
const unlinkFile = util.promisify(fs.unlink);

// Load environment variables from a .env file.
require('dotenv').config();

// AWS SDK for connecting to S3-compatible services like Cloudflare R2.
const { S3Client, PutObjectCommand, GetObjectCommand } = require("@aws-sdk/client-s3");

// Fluent-ffmpeg for video processing and thumbnail generation.
const ffmpeg = require('fluent-ffmpeg');

// --- NEW POSTGRES IMPORTS ---
const { Client } = require('pg');
// --- END NEW POSTGRES IMPORTS ---

// Set the path to the ffmpeg executable if it's not in your system's PATH.
// You might need this on certain platforms.
// ffmpeg.setFfmpegPath('/path/to/ffmpeg');

// --- START: ENSURE DIRECTORIES EXIST ON SERVER DEPLOY ---
// Multer and other file operations will fail if these directories aren't present.
// This ensures they are created every time the server starts.
const uploadDir = 'uploads/';
if (!fs.existsSync(uploadDir)) {
    fs.mkdirSync(uploadDir, { recursive: true });
}
const publicDir = 'public/';
if (!fs.existsSync(publicDir)) {
    fs.mkdirSync(publicDir, { recursive: true });
}
// --- END: ENSURE DIRECTORIES EXIST ---

// R2 credentials from environment variables for security.
const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME;
const R2_PUBLIC_URL = process.env.R2_PUBLIC_URL;

// --- POSTGRES CLIENT SETUP ---
let pgClient;

const connectToPostgres = async () => {
    try {
        pgClient = new Client({
            connectionString: process.env.DATABASE_URL,
            ssl: {
                rejectUnauthorized: false
            }
        });
        await pgClient.connect();
        console.log('Successfully connected to PostgreSQL!');
    } catch (error) {
        console.error('Failed to connect to PostgreSQL. Please check your DATABASE_URL environment variable.');
        console.error('Connection Error Details:', error);
        process.exit(1);
    }
};

const createTables = async () => {
    try {
        const createVideosTableQuery = `
            CREATE TABLE IF NOT EXISTS videos (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                creator VARCHAR(255) NOT NULL,
                video_url TEXT NOT NULL,
                thumbnail_url TEXT,
                uploaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `;
        const createUsersTableQuery = `
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL,
                display_name VARCHAR(255)
            );
        `;
        await pgClient.query(createVideosTableQuery);
        await pgClient.query(createUsersTableQuery);
        console.log('Videos and users tables created successfully (if they did not exist).');
    } catch (error) {
        console.error('Failed to create tables:', error);
        process.exit(1);
    }
}
// --- END POSTGRES SETUP ---


// Connect to the database and create index on server startup
const initializeServer = async () => {
    await connectToPostgres();
    await createTables();
    
    // Check if all necessary environment variables are set.
    if (!R2_ACCOUNT_ID || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY || !R2_BUCKET_NAME || !R2_PUBLIC_URL) {
        console.error("Error: Missing Cloudflare R2 credentials in the .env file.");
        console.error("Please ensure R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME, and R2_PUBLIC_URL are set.");
        process.exit(1); // Exit the process if credentials are not found.
    }
    
    // Create an instance of the Express application.
    const app = express();
    const port = 3000;

    // Middleware to parse JSON bodies
    app.use(express.json());

    // Set up the S3 client for Cloudflare R2.
    const r2 = new S3Client({
        region: 'auto', // Cloudflare R2 uses a special region.
        endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
        credentials: {
            accessKeyId: R2_ACCESS_KEY_ID,
            secretAccessKey: R2_SECRET_ACCESS_KEY,
        },
    });

    // Configure multer to store uploaded files to disk
    const upload = multer({ dest: 'uploads/' });

    // Middleware to serve static files from the 'public' directory.
    app.use(express.static(path.join(__dirname, 'public')));

    // Define routes for the different pages.
    app.get('/', (req, res) => {
        res.sendFile(path.join(__dirname, 'public', 'index.html'));
    });
    app.get('/creator', (req, res) => {
        res.sendFile(path.join(__dirname, 'public', 'creator.html'));
    });
    app.get('/upload', (req, res) => {
        res.sendFile(path.join(__dirname, 'public', 'upload.html'));
    });
    app.get('/watch', (req, res) => {
        res.sendFile(path.join(__dirname, 'public', 'watch.html'));
    });
    app.get('/login', (req, res) => {
        res.sendFile(path.join(__dirname, 'public', 'login.html'));
    });

    // --- UPDATED API ROUTES TO USE POSTGRES ---

    // API route to get a list of videos from Postgres.
    app.get('/api/videos', async (req, res) => {
        try {
            const result = await pgClient.query('SELECT * FROM videos ORDER BY uploaded_at DESC');
            res.json(result.rows);
        } catch (error) {
            console.error('Error fetching videos from Postgres:', error);
            res.status(500).json({ error: 'Failed to fetch videos' });
        }
    });

    // API route to get videos by creator name
    app.get('/api/videos/bycreator', async (req, res) => {
        const creatorName = req.query.name;
        if (!creatorName) {
            return res.status(400).send('Creator name is required.');
        }

        try {
            const query = 'SELECT * FROM videos WHERE creator = $1 ORDER BY uploaded_at DESC';
            const result = await pgClient.query(query, [creatorName]);
            res.json(result.rows);
        } catch (error) {
            console.error('Error fetching videos by creator from Postgres:', error);
            res.status(500).send('Error fetching videos.');
        }
    });

    // Login API using Postgres
    app.post('/api/login', async (req, res) => {
        try {
            const { username, password } = req.body;
            // NOTE: This is a simplified login. In a real application, you should use
            // password hashing (e.g., bcrypt) instead of plain text passwords.
            const query = 'SELECT * FROM users WHERE username = $1 AND password = $2';
            const result = await pgClient.query(query, [username, password]);
            
            if (result.rows.length > 0) {
                const user = result.rows[0];
                res.status(200).json({ message: 'Login successful', displayName: user.display_name });
            } else {
                res.status(401).json({ message: 'Invalid username or password' });
            }
        } catch (error) {
            console.error('Login error:', error);
            res.status(500).json({ message: 'An error occurred during login.' });
        }
    });

    // NEW ROUTE: Server-side proxy for fetching and serving thumbnails
    app.get('/api/thumbnails/:fileName', async (req, res) => {
        try {
            const { fileName } = req.params;
            const getCommand = new GetObjectCommand({
                Bucket: R2_BUCKET_NAME,
                Key: `thumbnails/${fileName}`
            });
            const { Body, ContentType } = await r2.send(getCommand);
            
            // Set the necessary headers for the browser
            res.set('Content-Type', ContentType);
            res.set('Cross-Origin-Resource-Policy', 'cross-origin');

            // Pipe the file stream to the response
            Body.pipe(res);
        } catch (error) {
            console.error('Error serving thumbnail:', error);
            res.status(404).send('Thumbnail not found.');
        }
    });

    // New route to handle video uploads with Cloudflare R2.
    app.post('/upload_video', upload.fields([
        { name: 'videoFile', maxCount: 1 },
        { name: 'thumbnailFile', maxCount: 1 }
    ]), async (req, res) => {
        // A temporary path for the uploaded files
        let originalVideoPath = null;
        let originalThumbnailPath = null;

        try {
            const videoFile = req.files.videoFile[0];
            const thumbnailFile = req.files.thumbnailFile ? req.files.thumbnailFile[0] : null;
            const videoTitle = req.body.videoTitle;
            const creatorName = req.body.creatorName;

            if (!videoFile || !videoTitle || !creatorName) {
                return res.status(400).send('Missing video file, title, or creator name.');
            }

            originalVideoPath = videoFile.path;
            
            let thumbnailFileName;
            let thumbnailBuffer;

            if (thumbnailFile) {
                originalThumbnailPath = thumbnailFile.path;
                // Convert custom thumbnail to PNG using ffmpeg
                thumbnailFileName = `thumb-custom-${Date.now()}.png`;
                thumbnailBuffer = await new Promise((resolve, reject) => {
                    const buffers = [];
                    ffmpeg()
                        .input(originalThumbnailPath)
                        .outputOptions('-f', 'image2pipe')
                        .outputOptions('-vcodec', 'png')
                        .on('error', (err) => {
                            console.error('Error converting custom thumbnail:', err);
                            reject(err);
                        })
                        .on('end', () => {
                            resolve(Buffer.concat(buffers));
                        })
                        .pipe(new stream.PassThrough())
                        .on('data', chunk => buffers.push(chunk))
                        .on('end', () => {});
                });
            } else {
                // Generate a thumbnail from the temp file on disk
                thumbnailFileName = `thumb-${Date.now()}.png`;
                thumbnailBuffer = await new Promise((resolve, reject) => {
                    const buffers = [];
                    ffmpeg()
                        .input(originalVideoPath)
                        .seekInput('0:05')
                        .frames(1)
                        .size('400x225')
                        .outputOptions('-f', 'image2pipe')
                        .outputOptions('-vcodec', 'png')
                        .on('error', (err) => {
                            console.error('Error generating thumbnail:', err);
                            reject(err);
                        })
                        .on('end', () => {
                            resolve(Buffer.concat(buffers));
                        })
                        .pipe(new stream.PassThrough())
                        .on('data', chunk => buffers.push(chunk))
                        .on('end', () => {});
                });
            }
            
            // Upload the thumbnail to R2
            const thumbnailUploadParams = {
                Bucket: R2_BUCKET_NAME,
                Key: `thumbnails/${thumbnailFileName}`,
                Body: thumbnailBuffer,
                ContentType: 'image/png',
            };
            await r2.send(new PutObjectCommand(thumbnailUploadParams));

            // --- VIDEO UPLOAD LOGIC ---
            const videoFileName = `video-${Date.now()}.mp4`;
            const videoUploadParams = {
                Bucket: R2_BUCKET_NAME,
                Key: `videos/${videoFileName}`,
                Body: fs.createReadStream(originalVideoPath),
                ContentType: 'video/mp4',
            };
            await r2.send(new PutObjectCommand(videoUploadParams));
            // --- END OF VIDEO UPLOAD LOGIC ---

            // --- NEW: Save video metadata to Postgres ---
            const videoUrl = `${R2_PUBLIC_URL}/videos/${videoFileName}`;
            const thumbnailUrl = `${req.protocol}://${req.get('host')}/api/thumbnails/${thumbnailFileName}`;
            const insertQuery = `
                INSERT INTO videos (title, creator, video_url, thumbnail_url)
                VALUES ($1, $2, $3, $4);
            `;
            await pgClient.query(insertQuery, [videoTitle, creatorName, videoUrl, thumbnailUrl]);
            // --- END NEW ---

            res.status(200).send('Video and metadata uploaded successfully.');
        } catch (error) {
            console.error('Upload Error:', error);
            res.status(500).send('Video upload failed.');
        } finally {
            // Clean up all temporary files on disk
            if (originalVideoPath) {
                await unlinkFile(originalVideoPath).catch(err => console.error('Error deleting original temp video file:', err));
            }
            if (originalThumbnailPath) {
                await unlinkFile(originalThumbnailPath).catch(err => console.error('Error deleting original temp thumbnail file:', err));
            }
        }
    });

    // Start the server and listen for requests on the specified port.
    app.listen(process.env.PORT || port, () => {
        console.log(`Server listening at http://localhost:${process.env.PORT || port}`);
    });
};

initializeServer();
