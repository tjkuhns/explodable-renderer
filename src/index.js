'use strict';

const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const ffmpeg = require('fluent-ffmpeg');
const { v4: uuidv4 } = require('uuid');
const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3001;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const RENDER_SECRET = process.env.RENDER_SECRET || '';

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', ts: new Date().toISOString() });
});

// Main render endpoint
app.post('/render', async (req, res) => {
  // Auth check
  const authHeader = req.headers['x-render-secret'] || '';
  if (RENDER_SECRET && authHeader !== RENDER_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { box_id, video_id, clip_suggestion_id, storage_path, start_time, end_time } = req.body;
  if (!box_id || !video_id || !clip_suggestion_id || !storage_path || start_time == null || end_time == null) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  console.log('[render] job received', { box_id, video_id, clip_suggestion_id, start_time, end_time });

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  // Acknowledge immediately — process async
  res.json({ status: 'queued', box_id });

  try {
    // 1. Mark box as processing
    await supabase.from('the_box').update({ status: 'processing', render_started_at: new Date().toISOString() }).eq('id', box_id);

    // 2. Get signed URL for raw video
    const { data: signedData, error: signedError } = await supabase.storage
      .from('videos')
      .createSignedUrl(storage_path, 3600);

    if (signedError || !signedData) {
      throw new Error('Failed to get signed URL: ' + (signedError?.message || 'no data'));
    }

    const signedUrl = signedData.signedUrl;
    console.log('[render] got signed URL');

    // 3. Download raw video to temp
    const tmpDir = os.tmpdir();
    const inputPath = path.join(tmpDir, video_id + '_input.mp4');
    const outputPath = path.join(tmpDir, clip_suggestion_id + '_output.mp4');

    await downloadFile(signedUrl, inputPath);
    console.log('[render] downloaded raw video to', inputPath);

    // 4. Run FFmpeg to cut clip
    const duration = parseFloat(end_time) - parseFloat(start_time);
    await runFfmpeg(inputPath, outputPath, parseFloat(start_time), duration);
    console.log('[render] ffmpeg done, output at', outputPath);

    // 5. Upload rendered clip to Supabase Storage
    const clipStoragePath = 'clips/' + clip_suggestion_id + '.mp4';
    const fileBuffer = fs.readFileSync(outputPath);
    const { error: uploadError } = await supabase.storage
      .from('videos')
      .upload(clipStoragePath, fileBuffer, { contentType: 'video/mp4', upsert: true });

    if (uploadError) throw new Error('Upload failed: ' + uploadError.message);
    console.log('[render] uploaded clip to', clipStoragePath);

    // 6. Insert into clips table
    await supabase.from('clips').insert({
      id: uuidv4(),
      video_id,
      clip_suggestion_id,
      storage_path: clipStoragePath,
      duration_seconds: duration,
      created_at: new Date().toISOString()
    });

    // 7. Update the_box to complete
    await supabase.from('the_box').update({
      status: 'complete',
      render_completed_at: new Date().toISOString(),
      output_storage_path: clipStoragePath
    }).eq('id', box_id);

    // 8. Update video status to complete
    await supabase.from('videos').update({ status: 'complete' }).eq('id', video_id);

    console.log('[render] job complete', box_id);

    // Cleanup temp files
    try { fs.unlinkSync(inputPath); } catch {}
    try { fs.unlinkSync(outputPath); } catch {}

  } catch (err) {
    console.error('[render] error', err.message);
    const supabase2 = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
    await supabase2.from('the_box').update({ status: 'failed' }).eq('id', box_id);
    await supabase2.from('videos').update({ status: 'failed' }).eq('id', video_id);
  }
});

function downloadFile(url, dest) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);
    const protocol = url.startsWith('https') ? https : http;
    protocol.get(url, (response) => {
      if (response.statusCode === 302 || response.statusCode === 301) {
        file.close();
        return downloadFile(response.headers.location, dest).then(resolve).catch(reject);
      }
      response.pipe(file);
      file.on('finish', () => file.close(resolve));
    }).on('error', (err) => {
      fs.unlink(dest, () => {});
      reject(err);
    });
  });
}

function runFfmpeg(input, output, startTime, duration) {
  return new Promise((resolve, reject) => {
    ffmpeg(input)
      .setStartTime(startTime)
      .setDuration(duration)
      .outputOptions('-c copy')
      .output(output)
      .on('end', resolve)
      .on('error', reject)
      .run();
  });
}

app.listen(PORT, () => {
  console.log('[explodable-renderer] listening on port', PORT);
});
