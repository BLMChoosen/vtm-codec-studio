# VTM Codec Studio

A modern desktop application for encoding and decoding **VVC (Versatile Video Coding)** bitstreams using the **VTM reference software** (VVenC/VVdeC compatible).

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![PySide6](https://img.shields.io/badge/GUI-PySide6-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

---

## Features

- **Encoder Interface** — Full parameter control: input YUV, main config, per-sequence config, frames, QP, output folder, and filename composition
- **Decoder Interface** — Queue-based .bin → .yuv decoding with real-time log output
- **Real-time Logs** — Scrollable terminal panel with live process output
- **Progress Indicator** — Queue-aware progress estimation for encoder, decoder, and converter
- **Execution Artifacts Folder (Encoder)** — Choose one base folder and automatically generate `reports/`, `tracefiles/`, and `metrics/`
- **TXT Reports (Encoder)** — Save a full report (`.txt`) for each queue execution
- **Tracefiles (Encoder)** — Save one VTM trace file (`.csv`) for each queue execution
- **CSV Reports** — Save one CSV metrics file for each queue execution in Encoder, plus per-job CSV output in Decoder
- **Preset System** — Save, load, and delete named encoder parameter presets
- **Encode Queue** — Queue multiple encoding jobs and run them in parallel with configurable worker count and per-execution artifacts
- **Decode Queue** — Queue multiple decoding jobs and run them in parallel with configurable worker count and per-job CSV outputs
- **Convert Queue** — Queue multiple `.y4m` conversion jobs and run them in parallel with configurable worker count
- **Compression Profiles** — Save reusable compression parameters (config, sequence cfg, frames, QP)
- **YUView Preview (Decode)** — Open reconstructed output `.yuv` directly in YUView
- **Recent Files** — Quick access to recently used input/output files
- **Drag & Drop** — Drop files directly onto input fields
- **Settings Panel** — Configure VTM paths (root, cfg folder, executables)
- **Dark Theme** — Premium dark UI inspired by modern video-editing software
- **Graceful Error Handling** — Validation, error messages, and process cancellation

---

## Project Structure

```text
Pythoncodec/
├── main.py                  # Application entry point
├── requirements.txt         # Python dependencies
├── README.md
├── core/                    # Business logic
│   ├── __init__.py
│   ├── encoder.py           # EncoderWorker (QThread)
│   ├── decoder.py           # DecoderWorker (QThread)
│   └── process_runner.py    # Base subprocess runner
├── ui/                      # GUI components
│   ├── __init__.py
│   ├── main_window.py       # Main window with tabs
│   ├── encoder_tab.py       # Encoder UI panel
│   ├── decoder_tab.py       # Decoder UI panel
│   ├── settings_dialog.py   # Settings configuration dialog
│   ├── theme.py             # Dark theme stylesheet
│   └── widgets.py           # Reusable custom widgets
└── utils/                   # Utilities
    ├── __init__.py
    ├── config.py             # Settings persistence (JSON)
    ├── validators.py         # Input validation helpers
   ├── presets.py            # Preset + compression profile save/load/delete
   ├── preview.py            # YUView integration helpers
   └── csv_export.py         # Metrics CSV export helper
```

---

## Installation

### Prerequisites

- **Python 3.10+** installed
- **VTM reference software** compiled (you need `EncoderAppStatic.exe` and `DecoderAppStatic.exe`)

### Steps

1. **Clone or download** this project.

2. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

3. **Run the application:**

   ```bash
   python main.py
   ```

4. **Configure VTM paths** (first-time setup):
   - Go to **File → Settings** (or press `Ctrl+,`)
   - Set the path to `EncoderAppStatic.exe`
   - Set the path to `DecoderAppStatic.exe`
   - Set the path to **YUView.exe** (optional but recommended)
   - Set the **Config Folder** (where `encoder_intra_vtm.cfg`, etc. live)
   - Optionally set the **VTM Root Folder**
   - Click **Save**

---

## Usage

### Encoding Queue (Queue Only)

1. Switch to the **Encoder** tab
2. Browse for your **input .yuv** file (or drag & drop)
3. Select the **main configuration** from the dropdown
4. (Optional) Browse for a **per-sequence .cfg** file
5. Set **Frames** and **QP** values
6. In **Output**, choose the **Pasta .bin**
7. In **Formato do Nome do Arquivo**, mark the checkboxes you want in the filename:
   - **Personalizado** (user-defined text)
   - **Quantization**
   - **Frames**
   - **Nome do YUV**
8. If **Personalizado** is checked, type the custom value (for example: `teste`)
9. Click **+ Add Current Settings**
10. Repeat for all jobs you want to process
11. In **Execution Artifacts**, choose **Pasta de artefatos** (base folder)
12. Set **Parallel Jobs** (number of concurrent workers)
13. Click **▶ Start Queue** to run jobs in parallel
14. Use **■ Cancel** to stop active jobs and cancel the remaining queue

If all naming checkboxes are selected and values are:

- Personalizado: `teste`
- QP: `22`
- Frames: `17`
- Input YUV: `bqsquare.yuv`

The output bitstream filename will be:

```text
teste-q22-f17-bqsquare.bin
```

When queue execution starts, the app creates these artifact folders inside the selected base path:

```text
reports/     -> one .txt report per execution
tracefiles/  -> one .csv VTM trace file per execution
metrics/     -> one .csv metrics file per execution
```

**Command executed:**

```bash
EncoderAppStatic.exe -c <main_cfg> -c <sequence_cfg> -i <input.yuv> -f <frames> -q <qp> -b <output.bin> --TraceFile=<trace.csv> --TraceRule="D_BLOCK_STATISTICS_CODED:poc>=0"
```

### Decoding Queue (Queue Only)

1. Switch to the **Decoder** tab
2. Browse for the **input .bin** file
3. Choose an **output .yuv** path
4. Choose an **output .csv** path (metrics report)
5. Click **+ Add Current Settings**
6. Repeat for all jobs you want to process
7. Set **Parallel Jobs** (number of concurrent workers)
8. Click **▶ Start Queue** to run jobs in parallel
9. Use **■ Cancel** to stop active jobs and cancel the remaining queue

**Command executed:**

```bash
DecoderAppStatic.exe -b <input.bin> -o <output.yuv>
```

### Conversion Queue (Queue Only)

1. Switch to the **Converter** tab
2. Browse for **input .y4m**
3. Review/edit auto-filled **output .yuv** and **sequence .cfg** paths
4. Set **Level**
5. Click **+ Add Current Settings**
6. Repeat for all jobs you want to process
7. Set **Parallel Jobs** (number of concurrent workers)
8. Click **▶ Start Queue** to run jobs in parallel
9. Use **■ Cancel** to stop active jobs and cancel the remaining queue

**Command executed:**

```bash
ffmpeg -y -i <input.y4m> -pix_fmt <pix_fmt> -f rawvideo <output.yuv>
```

### Presets

- After filling in encoder parameters, click **Save** in the Presets section
- Give the preset a name → it's saved to `~/.vtm_codec_studio/presets/`
- Later, select the preset from the dropdown and click **Load** to restore all fields
- Click **Delete** to remove a preset

### Compression Profiles

- Use **Compression Profiles** in the Encoder tab to store reusable compression parameters only
- Profiles are saved to `~/.vtm_codec_studio/compression_profiles/`
- Loading a profile restores main config, sequence config, frames, and QP

### YUView Preview

- Configure YUView in **Settings**
- In **Decoder**, use **Preview YUV in YUView** to open the reconstructed output file
- The app launches YUView with the selected `.yuv` path

---

## Configuration Storage

All settings and presets are saved locally:

| Item     | Location                                                 |
|----------|----------------------------------------------------------|
| Settings | `~/.vtm_codec_studio/settings.json`                      |
| Presets  | `~/.vtm_codec_studio/presets/<name>.json`                |
| Profiles | `~/.vtm_codec_studio/compression_profiles/<name>.json`   |

---

## Keyboard Shortcuts

| Shortcut  | Action          |
|-----------|-----------------|
| `Ctrl+,`  | Open Settings   |
| `Ctrl+Q`  | Quit            |

---

## Requirements

- Python 3.10+
- PySide6 >= 6.6.0
- Windows or Linux (adaptable to macOS with minor changes)

---

## License

MIT License — feel free to use, modify, and distribute.
