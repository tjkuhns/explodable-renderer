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
// Expected body: { box_id, video_id, clip_suggestion_id, user_id, storage_path, start_time, end_time }
app.post('/render', async (req, res) => {
    const authHeader = req.headers['x-render-secret'] || '';
    if (RENDER_SECRET && authHeader !== RENDER_SECRET) {
          return res.status(401).json({ error: 'Unauthorized' });
    }

           const { box_id, video_id, clip_suggestion_id, user_id, storage_path, start_time, end_time } = req.body;
    if (!box_id || !video_id || !clip_suggestion_id || !user_id || !storage_path || start_time == null || end_time == null) {
          return res.status(400).json({ error: 'Missing required fields' });
    }

           console.log('[render] job received', { box_id, video_id, clip_suggestion_id, start_time, end_time });

           const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

           // Acknowledge immediately - process async
           res.json({ status: 'queued', box_id });

           // Find the clip record for this box
           const clipId = uuidv4();
    try {
          // 1. Insert a 'processing' clips row
      await supabase.from('clips').insert({
              id: clipId,
              clip_suggestion_id,
              box_id,
              user_id,
              status: 'processing',
      });

      // 2. Get signed URL for raw video
      const { data: signedData, error: signedError } = await supabase.storage
            .from('videos')
            .createSignedUrl(storage_path, 3600);
          if (signedError || !signedData) {
                  throw new Error('Failed to get signed URL: ' + (signedError?.message || 'no data'));
          }
          const signedUrl = signedData.signedUrl;
          console.log('[render] got signed URL');

      // 3. Download video to temp file
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

      // 6. Update clips row to ready
      await supabase.from('clips').update({
              status: 'ready',
              storage_path: clipStoragePath,
              duration_seconds: duration,
      }).eq('id', clipId);

      // 7. Check if ALL clips for this box are ready — if so, mark box complete
      const { data: allClips, error: clipsCheckError } = await supabase
            .from('clips')
            .select('status')
            .eq('box_id', box_id);

      if (!clipsCheckError && allClips && allClips.length > 0) {
              const allReady = allClips.every(c => c.status === 'ready');
              if (allReady) {
                        await supabase.from('the_box').update({ status: 'complete' }).eq('id', box_id);
                        console.log('[render] all clips ready — box marked complete', box_id);
              } else {
                        console.log('[render] waiting for other clips, box stays generating', box_id);
              }
      }

      console.log('[render] job complete', box_id);

      // Cleanup temp files
      try { fs.unlinkSync(inputPath); } catch {}
          try { fs.unlinkSync(outputPath); } catch {}

    } catch (err) {
          console.error('[render] error:', err);
          await supabase.from('clips').update({
                  status: 'error',
                  error_message: err.message,
          }).eq('id', clipId).catch(() => {});
    }
});

function downloadFile(url, destPath) {
    return new Promise((resolve, reject) => {
          const protocol = url.startsWith('https') ? https : http;
          const file = fs.createWriteStream(destPath);
          protocol.get(url, (response) => {
                  if (response.statusCode === 301 || response.statusCode === 302) {
                            file.close();
                            fs.unlink(destPath, () => {});
                            return downloadFile(response.headers.location, destPath).then(resolve).catch(reject);
                  }
                  response.pipe(file);
                  file.on('finish', () => file.close(resolve));
          }).on('error', (err) => {
                  fs.unlink(destPath, () => {});
                  reject(err);
          });
    });
}

function runFfmpeg(inputPath, outputPath, startTime, duration) {
    return new Promise((resolve, reject) => {
          ffmpeg(inputPath)
            .setStartTime(startTime)
            .setDuration(duration)
            .output(outputPath)
            .videoCodec('copy')
            .audioCodec('copy')
            .on('end', resolve)
            .on('error', reject)
            .run();
    });
}

app.listen(PORT, () => {
    console.log('[render] server listening on port', PORT);
});
