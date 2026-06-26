# Synaptica

**AI-Based Cognitive Mental Health Estimator for Developers**

Synaptica passively observes coding behavior inside VS Code — typing rhythm, errors, debugging struggles, mouse movement, and application usage — and converts this behavioral telemetry into real-time estimates of **focus, fatigue, cognitive load, stress, and burnout risk**. It closes the loop with personalized, AI-generated recommendations delivered through a live dashboard.

---

## Table of Contents

- [Problem Statement](#problem-statement)
- [Key Features](#key-features)
- [System Architecture](#system-architecture)
- [Component Overview](#component-overview)
- [Tech Stack](#tech-stack)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [API Reference](#api-reference)
- [Database Schema](#database-schema)
- [ML/DL Models](#mldl-models)
- [Privacy & Consent](#privacy--consent)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

---

## Problem Statement

Developers frequently work under high cognitive load for extended periods with no objective signal of mental fatigue, declining focus, or burnout risk until productivity or health visibly suffers. Existing wellness tools rely on **self-reported surveys** — infrequent, subjective, and easy to ignore.

Synaptica addresses this by:

- Passively capturing **IDE-level behavioral signals** (typing rhythm, error patterns, debugging recovery, mouse hesitation) in real time.
- Distinguishing **genuine cognitive fatigue** from normal individual variation — a fast typist isn't necessarily focused, a slow typist isn't necessarily fatigued.
- Using behavioral data to **predict cognitive states** via ML/DL models instead of static rule thresholds.
- Providing **actionable, real-time interventions** rather than retrospective reports.

---

## Key Features

- 🔹 Real-time coding activity capture (save, edit, debug, compile)
- 🔹 Keyboard & mouse behavioral analytics (speed, hesitation, error rate)
- 🔹 AST-based "cognitive blunder" detection (off-by-one, assignment-vs-equality, null references, logic reversal)
- 🔹 Debugging struggle & recovery efficiency tracking
- 🔹 Personalized baseline learning (autoencoder-based anomaly detection)
- 🔹 Focus, Fatigue, Cognitive Load, and Burnout prediction models
- 🔹 Fusion engine producing one unified Mental Health State
- 🔹 Hybrid rule + AI recommendation engine
- 🔹 Live dashboard with trend graphs and productivity heatmaps
- 🔹 Kafka-based streaming architecture for scalable, low-latency processing

---

## System Architecture

```
 Developer's Machine                         Backend Cluster                        Client
┌─────────────────────┐         ┌─────────────────────────────────────┐    ┌──────────────────┐
│ VS Code Extension     │         │ Event Streaming Layer (Kafka/Redis) │    │ React Dashboard  │
│ Activity Collector    │  WS    │                                     │ WS │                  │
│ Keyboard/Mouse Hooks  │ ─────► │ Feature Engineering Engine          │───►│ Cognitive Health │
└─────────────────────┘         │ Baseline Learning (Autoencoder)     │    │ Burnout Trends   │
                                  │ Focus / Fatigue / Load / Burnout    │    │ Productivity Map │
                                  │ Mental Health Fusion (XGBoost)      │    └──────────────────┘
                                  │ Recommendation Engine               │
                                  └──────────────┬──────────────────────┘
                                                  │
                                  ┌───────────────▼──────────────────────┐
                                  │ PostgreSQL (durable) + Redis (live)  │
                                  └───────────────────────────────────────┘
```

**End-to-end data flow:**

```
Developer Action
   → VS Code / OS Hooks
   → Event Serialization
   → WebSocket
   → Kafka Topic
   → Stream Consumers
   → Feature Engineering
   → Baseline Deviation Check
   → Cognitive Models (Focus / Fatigue / Load / Burnout)
   → Mental Health Fusion Engine
   → Recommendation Engine
   → Redis (live cache) + PostgreSQL (history)
   → Dashboard (WebSocket push)
   → User Feedback Loop → refines Baseline Engine
```

### Component Connection Map

```
[VS Code Ext] ─┐
[Activity Collector] ─┤── WebSocket ──► [Kafka / Event Streaming Layer]
                │
                └──► raw events
                         │
                         ▼
        [Keyboard Analytics] [Mouse Analytics] [Error Monitor] [Blunder Detector] [Debug Struggle Analyzer]
                         │   (parallel feature extractors on the same event stream)
                         ▼
              [Feature Engineering Engine]
                         │
                         ▼
            [Personal Baseline Learning Engine] ──► anomaly_score
                         │
                         ▼
   ┌─────────────┬───────────────┬──────────────┬───────────────┐
   ▼             ▼               ▼              ▼
[Focus Engine] [Fatigue Engine] [Load Engine] [Burnout Engine]
   └─────────────┴───────────────┴──────────────┴───────────────┘
                         │
                         ▼
              [Mental Health Fusion Engine]
                         │
                         ▼
              [Recommendation Engine]
                         │
                ┌────────┴────────┐
                ▼                 ▼
        [PostgreSQL: history]  [Redis: live cache]
                         │
                         ▼
              [Dashboard] ◄── WebSocket push
                         │
                         ▼
              [User Feedback Loop] ──► refines Baseline Engine
```

---

## Component Overview

| # | Component | Objective |
|---|---|---|
| 1 | VS Code Extension | Primary data source — monitors coding events (save, edit, debug, compile) |
| 2 | Computer Activity Collector | Tracks window switching, app usage, idle time, session duration |
| 3 | Keyboard Analytics Engine | Extracts speed, error, and pause features from typing |
| 4 | Mouse Analytics Engine | Detects hesitation/uncertainty via motion, click, and jitter features |
| 5 | Error Monitoring Engine | Captures and categorizes compiler/runtime errors and warnings |
| 6 | Cognitive Blunder Detector | AST-based detection of fatigue-linked code mistakes *(most novel component)* |
| 7 | Debugging Struggle Analyzer | Measures cognitive recovery via edit → compile → error cycles |
| 8 | Feature Engineering Engine | Converts raw multi-source events into ML-ready vectors |
| 9 | Personal Baseline Learning Engine | Learns individual "normal" behavior using an autoencoder |
| 10 | Cognitive Load Estimation Engine | LSTM + Attention model predicting workload class |
| 11 | Fatigue Detection Engine | Transformer-based model predicting a fatigue score |
| 12 | Focus Estimation Engine | Dense neural network predicting a focus score |
| 13 | Burnout Prediction Engine | BiLSTM + Attention model forecasting burnout over 7/14/30-day windows |
| 14 | Mental Health Fusion Engine | XGBoost model combining all signals into one health state |
| 15 | Recommendation Engine | Hybrid rule + AI engine generating interventions |
| 16 | Dashboard & Visualization | React-based UI for cognitive health, burnout, and productivity insights |
| 17 | Data Storage Layer | PostgreSQL (durable) + Redis (live cache) |
| 18 | Real-Time Streaming Layer | Kafka/Redis Streams + WebSockets for continuous event flow |

<details>
<summary><b>Expand for detailed component specs</b></summary>

### 1. VS Code Extension
- **Monitors:** Code Typing, File Open/Save/Close, Undo/Redo, Refactoring, Debug Start/Stop, Compilation
- **Workflow:** `Developer Writes Code → VS Code API Captures Event → Event Serialized → Send To Backend`
- **Tech:** TypeScript, VS Code API, WebSocket, VSIX
- **Example event:**
  ```json
  { "event": "file_save", "file": "main.py", "lines_changed": 14, "timestamp": "10:31:02" }
  ```

### 2. Computer Activity Collector
- **Tracks:** Window Switching, Application Usage, Browser Usage, Idle Time, Session Duration
- **Workflow:** `Windows Hook → Foreground Window Detection → Application Log → Backend`
- **Tech:** Python, psutil, pywin32, pynput
- **Output:** `{ "active_app": "chrome", "duration": 180 }`

### 3. Keyboard Analytics Engine
- **Speed features:** WPM, KPM, Typing Burst
- **Error features:** Backspace Rate, Delete Rate, Undo Rate, Correction Rate
- **Pause features:** Average Pause, Longest Pause, Idle Frequency
- **Tech:** Pynput, NumPy, Pandas
- **Output:** `{ "wpm": 68, "backspace_rate": 12, "idle_time": 8 }`

### 4. Mouse Analytics Engine
- **Motion:** Mouse Speed, Acceleration, Path Length
- **Click behavior:** Single/Double Click, Misclick Rate
- **Jitter:** Movement Variance, Direction Changes
- **Tech:** Pynput, PyAutoGUI, NumPy
- **Output:** `{ "mouse_jitter": 0.71, "misclick_rate": 0.12 }`

### 5. Error Monitoring Engine
- **Captures:** Compiler Errors, Runtime Errors, Warnings, Build Failures
- **Supported languages:** Python, Java, C++, JavaScript
- **Workflow:** `Compiler Output → Error Parser → Categorizer → Database`
- **Output:** `{ "type": "runtime_error", "severity": "high" }`

### 6. Cognitive Blunder Detector *(Most Novel Component)*
- **Blunder types:** Off-By-One, Assignment Error (`if(a=b)`), Null Reference, Logic Reversal, Infinite Loop
- **Workflow:** `Source Code → AST Parser → Rule Engine → Blunder Score`
- **Tech:** Tree-sitter, Python, FastAPI
- **Output:** `{ "blunder_score": 78 }`

### 7. Debugging Struggle Analyzer
- **Metrics:** Fix Attempts, Repeated Errors, Recovery Time, Success Rate
- **Formula:** `Recovery Efficiency = Successful Fixes / Attempts`
- **Output:** `{ "recovery_efficiency": 62 }`

### 8. Feature Engineering Engine
- **Inputs:** Typing, Errors, Mouse, Switches, Sessions
- **Output vector:** `[typing_speed, backspace_rate, error_count, blunder_score, switch_count, session_length, recovery_time]`
- **Tech:** Pandas, NumPy, Scikit-learn

### 9. Personal Baseline Learning Engine
- **Why needed:** Developer A may average 90 WPM, Developer B 45 WPM — both can be healthy.
- **Workflow:** `User Data → Autoencoder → Normal Pattern → Deviation Detector`
- **Tech:** PyTorch (Autoencoders)
- **Output:** `{ "anomaly_score": 0.21 }`

### 10. Cognitive Load Estimation Engine
- **Inputs:** Typing Speed, Errors, Context Switches, Recovery Time
- **Architecture:** `Feature Sequence → LSTM → Attention → Dense Layer → Workload Class`
- **Output:** `{ "load": "High" }`

### 11. Fatigue Detection Engine
- **Inputs:** Pause Time, Typing Variance, Blunder Score, Recovery Time
- **Architecture:** `Transformer Encoder → Temporal Attention → Fatigue Score`
- **Output:** `{ "fatigue": 74 }`

### 12. Focus Estimation Engine
- **Inputs:** Idle Time, Context Switches, Typing Consistency
- **Architecture:** Dense Neural Network
- **Output:** `{ "focus": 82 }`

### 13. Burnout Prediction Engine
- **Data windows:** 7 / 14 / 30 days
- **Features:** Fatigue Trend, Error Trend, Work Hours, Recovery Trend
- **Architecture:** `BiLSTM → Attention → Burnout Prediction`
- **Output:** `{ "burnout": "Moderate" }`

### 14. Mental Health Fusion Engine
- **Inputs:** Focus, Fatigue, Load, Burnout, Stress, Recovery
- **Architecture:** `Feature Fusion → XGBoost → Final Health State`
- **Output:**
  ```json
  { "focus": 83, "fatigue": 42, "stress": 31, "burnout": 18 }
  ```

### 15. Recommendation Engine
- **Rule examples:**
  ```
  IF fatigue > 80: recommend_break()
  IF burnout > 70: stop_working()
  ```
- **AI suggestions:** "Take a 10-minute break", "Hydrate yourself", "Highest productivity: 10AM–1PM", "Recovery time increased by 48%"

### 16. Dashboard & Visualization
- **Tech:** React, TypeScript, Recharts, Material UI
- **Sections:** Cognitive Health (Focus/Fatigue/Stress), Coding Analytics (Errors/Blunders/Recovery), Burnout Analytics (Trend Graphs/Risk Levels), Productivity Heatmap (Best/Worst Coding Hours)

### 17. Data Storage Layer
- **PostgreSQL tables:** Users, Sessions, Events, Features, Predictions, Recommendations
- **Redis:** Live Metrics, Real-time Predictions, WebSocket Cache

### 18. Real-Time Streaming Layer
- **Workflow:** `Event Producer → Kafka → Consumers → AI Services`
- **Tech:** Apache Kafka, Redis Streams (lightweight alternative), WebSockets

</details>

---

## Tech Stack

| Layer | Technology |
|---|---|
| IDE Instrumentation | TypeScript, VS Code Extension API, VSIX packaging |
| Background Activity Tracking | Python, psutil, pywin32, pynput, PyAutoGUI |
| Messaging (IDE → Backend) | WebSocket |
| Real-Time Streaming | Apache Kafka (or Redis Streams) |
| Backend API | FastAPI (Python) |
| Feature Engineering | Pandas, NumPy, Scikit-learn |
| Code Analysis | Tree-sitter (AST parsing) |
| Deep Learning | PyTorch (Autoencoders, LSTM, Transformer) |
| Classical ML | XGBoost, Scikit-learn |
| Primary Database | PostgreSQL |
| Cache / Live Metrics | Redis |
| Frontend Dashboard | React, TypeScript, Recharts, Material UI |
| Auth | OAuth2 / JWT |
| Deployment | Docker, Docker Compose / Kubernetes |

---

## Repository Structure

```
synaptica/
├── extension/                 # VS Code extension (TypeScript)
│   ├── src/
│   └── package.json
├── activity-collector/        # OS-level activity tracker (Python)
├── backend/
│   ├── ingestion/              # WebSocket + Kafka producers
│   ├── feature_engineering/    # Pandas/NumPy pipelines
│   ├── models/
│   │   ├── baseline_autoencoder/
│   │   ├── focus_engine/
│   │   ├── fatigue_engine/
│   │   ├── load_engine/
│   │   ├── burnout_engine/
│   │   └── fusion_engine/
│   ├── blunder_detector/       # Tree-sitter AST analysis
│   ├── recommendation_engine/
│   ├── api/                    # FastAPI routes
│   └── db/                     # PostgreSQL models & migrations
├── dashboard/                  # React + TypeScript frontend
├── docker-compose.yml
├── .env.example
└── README.md
```

---

## Getting Started

### Prerequisites
- Node.js ≥ 18 and npm/yarn
- Python ≥ 3.10
- Docker & Docker Compose
- PostgreSQL, Redis, Kafka (or use the provided `docker-compose.yml`)

### Installation

```bash
# Clone the repository
git clone https://github.com/<your-org>/synaptica.git
cd synaptica

# Start backend infrastructure (Postgres, Redis, Kafka, FastAPI)
docker-compose up -d

# Install and build the VS Code extension
cd extension
npm install
npm run build
npm run package    # produces .vsix

# Install the extension into VS Code
code --install-extension synaptica-0.1.0.vsix

# Set up the activity collector
cd ../activity-collector
pip install -r requirements.txt
python collector.py

# Run the dashboard
cd ../dashboard
npm install
npm run dev
```

### Environment Variables

```env
DATABASE_URL=postgresql://user:pass@localhost:5432/synaptica
REDIS_URL=redis://localhost:6379
KAFKA_BROKER_URL=localhost:9092
JWT_SECRET=your-secret-key
WS_BACKEND_URL=ws://localhost:8000/ws
```

---

## API Reference

### REST Endpoints

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/v1/auth/register` | Register a developer account |
| POST | `/api/v1/auth/login` | Authenticate and receive a JWT |
| POST | `/api/v1/events/coding` | Ingest a VS Code event (fallback to WebSocket) |
| POST | `/api/v1/events/activity` | Ingest an OS-level activity event |
| GET | `/api/v1/features/{user_id}` | Get the latest computed feature vector |
| GET | `/api/v1/baseline/{user_id}` | Get personal baseline + anomaly score |
| GET | `/api/v1/cognitive-state/{user_id}` | Get Focus/Fatigue/Load snapshot |
| GET | `/api/v1/burnout/{user_id}?window=7d` | Get burnout trend over a window |
| GET | `/api/v1/mental-health/{user_id}` | Get fused mental health state |
| GET | `/api/v1/recommendations/{user_id}` | Get active recommendations |
| GET | `/api/v1/dashboard/summary/{user_id}` | Get aggregated dashboard payload |
| GET | `/api/v1/heatmap/{user_id}` | Get productivity heatmap data |

### WebSocket Channels

| Channel | Direction | Purpose |
|---|---|---|
| `ws/ingest/events` | Extension → Backend | Streams raw coding/activity events |
| `ws/dashboard/live` | Backend → Dashboard | Pushes live focus/fatigue/burnout updates |
| `ws/notifications` | Backend → Dashboard/Extension | Pushes break/recommendation alerts |

### Sample Payloads

**Ingest event** — `POST /api/v1/events/coding`
```json
{
  "user_id": "u123",
  "event": "file_save",
  "file": "main.py",
  "lines_changed": 14,
  "timestamp": "2026-06-26T10:31:02Z"
}
```

**Mental health state response**
```json
{
  "user_id": "u123",
  "focus": 83,
  "fatigue": 42,
  "stress": 31,
  "burnout": 18,
  "recovery_efficiency": 62,
  "recommendation": "Take a 10-minute break"
}
```

---

## Database Schema

```sql
Users(user_id PK, name, email, baseline_wpm, created_at)

Sessions(session_id PK, user_id FK, start_time, end_time, total_duration)

Events(event_id PK, user_id FK, session_id FK, event_type, payload JSONB, timestamp)

Features(feature_id PK, user_id FK, session_id FK, typing_speed, backspace_rate,
          error_count, blunder_score, switch_count, recovery_time, created_at)

Predictions(prediction_id PK, user_id FK, focus, fatigue, load, stress, burnout,
            model_version, created_at)

Recommendations(rec_id PK, user_id FK, prediction_id FK, message, triggered_rule, created_at)
```

**Redis key patterns:**
- `live:user:{id}:metrics` → latest feature vector (short TTL)
- `live:user:{id}:prediction` → latest fused state
- `stream:events:{user_id}` → Redis Stream / Kafka partition key

---

## ML/DL Models

| Engine | Model | Input | Output |
|---|---|---|---|
| Baseline Learning | Autoencoder | Behavioral feature vector | Anomaly score |
| Cognitive Load | LSTM + Attention | Feature sequence | Load class (Low/Med/High) |
| Fatigue | Transformer Encoder | Pause, variance, blunder, recovery | Fatigue score (0–100) |
| Focus | Dense NN | Idle time, switches, consistency | Focus score (0–100) |
| Burnout | BiLSTM + Attention | 7/14/30-day trend features | Burnout level |
| Fusion | XGBoost | All above outputs | Final mental health state |

---

## Privacy & Consent

Synaptica is workplace-adjacent behavioral monitoring software and should be treated with the same care as any biometric data system:

- Records **behavioral metadata only** (timing, counts, rates) — never actual keystroke content, screenshots, or clipboard data.
- Requires explicit **opt-in consent** on install, with a clear disclosure of exactly what is and isn't collected.
- Provides a configurable **monitoring on/off toggle** at all times.
- Stores data with per-user access controls; aggregate/team-level views (if added) should be anonymized.
- Recommends a data retention policy (e.g., raw events purged after N days, only aggregated features retained long-term).

> This system is a wellness aid, not a diagnostic or clinical tool. It does not diagnose mental health conditions and should not be used as a substitute for professional medical advice.

---

## Roadmap

- [ ] Multi-IDE support (JetBrains, Sublime Text)
- [ ] Anonymized, aggregate-only team burnout heatmaps for managers
- [ ] Federated learning so raw behavioral data never leaves the developer's machine
- [ ] Calendar/Slack integration for context-aware break scheduling
- [ ] Opt-in voice/sentiment analysis from stand-up call transcripts

---

## Contributing

Contributions are welcome.

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit your changes: `git commit -m "Add your feature"`
4. Push to the branch: `git push origin feature/your-feature`
5. Open a Pull Request

Please open an issue first for major changes to discuss what you'd like to change.

---

## License

This project is licensed under the MIT License — see the `LICENSE` file for details.

---

**Synaptica** — turning everyday coding behavior into actionable insight for developer wellbeing.
