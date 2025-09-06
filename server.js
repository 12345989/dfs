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

// Set the path to the ffmpeg executable if it's not in your system's PATH.
// You might need this on certain platforms.
// ffmpeg.setFfmpegPath('/path/to/ffmpeg');

// R2 credentials from environment variables for security.
const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME;

// Check if all necessary environment variables are set.
if (!R2_ACCOUNT_ID || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY || !R2_BUCKET_NAME) {
    console.error("Error: Missing Cloudflare R2 credentials in the .env file.");
    console.error("Please ensure R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, and R2_BUCKET_NAME are set.");
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

// Configure multer to store uploaded files in memory
const upload = multer({ storage: multer.memoryStorage() });

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

// Helper function to read the video metadata from R2.
async function readVideoData() {
    try {
        const getCommand = new GetObjectCommand({
            Bucket: R2_BUCKET_NAME,
            Key: 'videos.json'
        });
        const { Body } = await r2.send(getCommand);
        const data = await new Response(Body).text();
        return JSON.parse(data);
    } catch (error) {
        // If the file is not found, return an empty array.
        if (error.Code === 'NoSuchKey' || error.name === 'NoSuchKey') {
            return [];
        }
        console.error('Error reading video data from R2:', error);
        return [];
    }
}

// Helper function to write the video metadata to R2.
async function writeVideoData(videos) {
    try {
        const putCommand = new PutObjectCommand({
            Bucket: R2_BUCKET_NAME,
            Key: 'videos.json',
            Body: JSON.stringify(videos, null, 2),
            ContentType: 'application/json'
        });
        await r2.send(putCommand);
    } catch (error) {
        console.error('Error writing video data to R2:', error);
    }
}

// Helper function to read the user metadata from R2.
async function readUserData() {
    try {
        const getCommand = new GetObjectCommand({
            Bucket: R2_BUCKET_NAME,
            Key: 'users.json'
        });
        const { Body } = await r2.send(getCommand);
        const data = await new Response(Body).text();
        return JSON.parse(data);
    } catch (error) {
        // If the file is not found, return an empty array.
        if (error.Code === 'NoSuchKey' || error.name === 'NoSuchKey') {
            return [];
        }
        console.error('Error reading user data from R2:', error);
        return [];
    }
}

// API route to get a list of videos from the JSON file on R2.
app.get('/api/videos', async (req, res) => {
    const videos = await readVideoData();
    res.json(videos);
});

// NEW API route to get videos by creator name
app.get('/api/videos/bycreator', async (req, res) => {
    const creatorName = req.query.name;
    if (!creatorName) {
        return res.status(400).send('Creator name is required.');
    }

    try {
        const videos = await readVideoData();
        const filteredVideos = videos.filter(video => video.creator === creatorName);
        res.json(filteredVideos);
    } catch (error) {
        console.error('Error fetching videos by creator:', error);
        res.status(500).send('Error fetching videos.');
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

// NEW ROUTE: Server-side proxy for fetching and serving videos
app.get('/api/videos/:fileName', async (req, res) => {
    try {
        const { fileName } = req.params;
        const getCommand = new GetObjectCommand({
            Bucket: R2_BUCKET_NAME,
            Key: `videos/${fileName}`
        });
        const { Body, ContentType, ContentLength } = await r2.send(getCommand);
        
        res.set('Content-Type', ContentType);
        res.set('Content-Length', ContentLength);
        res.set('Cross-Origin-Resource-Policy', 'cross-origin');

        Body.pipe(res);
    } catch (error) {
        console.error('Error serving video:', error);
        res.status(404).send('Video not found.');
    }
});


// New route to handle video uploads with Cloudflare R2.
app.post('/upload_video', upload.fields([
    { name: 'videoFile', maxCount: 1 },
    { name: 'thumbnailFile', maxCount: 1 }
]), async (req, res) => {
    try {
        const videoFile = req.files.videoFile[0];
        const thumbnailFile = req.files.thumbnailFile ? req.files.thumbnailFile[0] : null;
        const videoTitle = req.body.videoTitle;
        const creatorName = req.body.creatorName;

        if (!videoFile || !videoTitle || !creatorName) {
            return res.status(400).send('Missing video file, title, or creator name.');
        }

        let thumbnailFileName;
        let thumbnailBuffer;

        if (thumbnailFile) {
            thumbnailFileName = `${Date.now()}-${thumbnailFile.originalname}`;
            thumbnailBuffer = thumbnailFile.buffer;
        } else {
            thumbnailFileName = `thumb-${Date.now()}.png`;
            thumbnailBuffer = await new Promise((resolve, reject) => {
                const buffers = [];
                ffmpeg()
                    .input(stream.Readable.from(videoFile.buffer))
                    .seekInput('0:05') // Seek to 5 seconds to get a frame
                    .frames(1) // Get a single frame
                    .size('400x225')
                    .outputOptions('-f', 'image2pipe') // Output as a single image pipe
                    .outputOptions('-vcodec', 'png') // Set output codec to png
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

        const videoFileName = `video-${Date.now()}.mp4`;
        const convertedVideoBuffer = await new Promise((resolve, reject) => {
            const buffers = [];
            ffmpeg()
                .input(stream.Readable.from(videoFile.buffer))
                .outputOptions('-vcodec', 'libx264', '-acodec', 'aac') // Convert to MP4
                .on('error', (err) => {
                    console.error('Error converting video:', err);
                    reject(err);
                })
                .on('end', () => {
                    console.log('Video converted to MP4 successfully.');
                    resolve(Buffer.concat(buffers));
                })
                .pipe(new stream.PassThrough())
                .on('data', chunk => buffers.push(chunk))
                .on('end', () => {});
        });

        const videoUploadParams = {
            Bucket: R2_BUCKET_NAME,
            Key: `videos/${videoFileName}`,
            Body: convertedVideoBuffer,
            ContentType: 'video/mp4',
        };
        await r2.send(new PutObjectCommand(videoUploadParams));

        // Save the video metadata to our JSON file.
        const videos = await readVideoData();
        const newVideo = {
            id: `vid${Date.now()}`,
            title: videoTitle,
            creator: creatorName,
            // Use the dynamic host to create the URL
            videoUrl: `${req.protocol}://${req.get('host')}/api/videos/${videoFileName}`,
            // We now point the thumbnail to the new local proxy route.
            thumbnailUrl: `${req.protocol}://${req.get('host')}/api/thumbnails/${thumbnailFileName}`
        };
        videos.push(newVideo);
        await writeVideoData(videos);

        res.status(200).send('Video and metadata uploaded successfully.');
    } catch (error) {
        console.error('Upload Error:', error);
        res.status(500).send('Video upload failed.');
    }
});

app.post('/api/login', async (req, res) => {
    try {
        const { username, password } = req.body;
        const users = await readUserData();

        const user = users.find(u => u.username === username && u.password === password);
        if (user) {
            res.json({ message: 'Login successful', displayName: user.displayName });
        } else {
            res.status(401).json({ message: 'Invalid username or password' });
        }
    } catch (error) {
        console.error('Login error:', error);
        res.status(500).json({ message: 'An error occurred during login.' });
    }
});

// Start the server and listen for requests on the specified port.
app.listen(process.env.PORT || port, () => {
    console.log(`Server listening at http://localhost:${process.env.PORT || port}`);
});
