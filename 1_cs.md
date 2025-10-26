# **RAG Email System — Voiceover Cue Sheet**

All cues assume ~115 WPM delivery (≈4 min total).

| **Scene**                              | **File name**            | **Approx. duration** | **Start / End cues**                                      | **Notes / Audio direction**                                                                                                               |
| -------------------------------------- | ------------------------ | -------------------- | --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| **1. Context – Why We’re Building It** | `scene01_context.mp3`    | 0:30 s               | **Start:** Soft fade-in 0.5 s  •  **End:** Fade-out 0.5 s | Calm, explanatory tone. Slight lift on “grounded draft replies” to underscore intent.                                                     |
| **2. Purpose of This Phase**           | `scene02_purpose.mp3`    | 0:30 s               | Fade-in / out 0.5 s                                       | Neutral cadence. Emphasize “readiness, not performance claims.” 1 s pause before next scene.                                              |
| **3. System Overview**                 | `scene03_overview.mp3`   | 0:30 s               | Fade-in 0.3 s                                             | Maintain even pacing through workflow steps. Small pause after each component name (Gmail – Workato – Vertex AI – Cloud Storage – Gmail). |
| **4. What’s Working Now**              | `scene04_working.mp3`    | 0:30 s               | Fade-in / out 0.5 s                                       | Slight emphasis on “Every action records telemetry.” 0.5 s rest between sentences to allow text overlays.                                 |
| **5. Preparing for Testing**           | `scene05_preparing.mp3`  | 0:45 s               | Fade-in 0.3 s  •  End fade-out 0.7 s                      | Mild forward energy; tone of anticipation. Pause after “but not sent.”                                                                    |
| **6. Governance and Safety**           | `scene06_governance.mp3` | 0:30 s               | Fade-in 0.3 s                                             | Lower tone slightly; reassure rather than sell. Clean articulation on “least-privilege access.”                                           |
| **7. What Success Looks Like**         | `scene07_success.mp3`    | 0:30 s               | Fade-in / out 0.5 s                                       | Pace steady, deliberate. Pause before “These results will determine readiness.”                                                           |
| **8. Next Steps**                      | `scene08_nextsteps.mp3`  | 0:15 s               | Fade-in 0.3 s  •  End fade-out 0.7 s                      | Light upward inflection on “controlled testing.” Gentle close toward “reliable automation.”                                               |
| **9. Closing Tagline**                 | `scene09_tagline.mp3`    | 0:05 s               | Fade-in 0.2 s  •  Hold 1 s silence after                  | Say: *“RAG Email System.  Grounded automation for real work.”*  Slightly slower pace, confident close.                                    |

---

## **Global audio specs**

| Setting             | Recommended value                                       |
| ------------------- | ------------------------------------------------------- |
| **Voice**           | `en-US-Wavenet-D` or `en-US-Wavenet-F` (neutral, clear) |
| **Speed**           | `1.0` (≈115 WPM)                                        |
| **Pitch**           | `-2 st` for calm authority                              |
| **Audio format**    | MP3 @ 192 kbps (mono)                                   |
| **Silence padding** | 0.5 s lead-in, 0.5–1.0 s tail-out per clip              |

---

### **FFmpeg alignment reference**

When stitching video segments (Option B workflow):

```bash
ffmpeg -i scene01_context.mp3 -i scene01.mp4 -c:v copy -c:a aac -shortest out01.mp4
```

Repeat for each scene; then concatenate:

```bash
printf "file 'out01.mp4'\nfile 'out02.mp4'\n..." > list.txt
ffmpeg -f concat -safe 0 -i list.txt -c copy rag_pretest_final.mp4
```

---

### **Sound bed suggestion (optional)**

* Ambient tone: 40–60 Hz pad, low volume (−28 LUFS), continuous across scenes 1–8, drop 2 s before tagline.
* No percussion, no melody; think background air, not music.

---

### **QC checklist**

* ✅ All narration synced ±0.2 s to scene visuals.
* ✅ Fade transitions match slide dissolves.
* ✅ No peak > −3 dBFS.
* ✅ Export master → `rag_pretest_final_mix.mp4` for internal Drive share.

---
