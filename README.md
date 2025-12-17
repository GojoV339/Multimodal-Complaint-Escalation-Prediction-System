# 📌 Multimodal Complaint Priority Prediction System

This project predicts customer complaint escalation & priority using both **text** (complaint narrative) and **tabular metadata** (product, issue, company response, etc.).  
Designed to simulate **real industry-level ML engineering practices** with a modular, scalable architecture.

---

## 🚀 Key System Capabilities (Planned)

✔ Automated data ingestion & versioned storage (Bronze → Silver → Gold layers)  
✔ Text + Tabular feature fusion pipeline  
✔ Model training, hyperparameter configuration & evaluation  
✔ Model persistence & registry for deployment  
✔ CLI and API interfaces for prediction  
✔ Clear logs, tests, and documentation  

Future:
➡️ Cloud integration (S3, SQS, Lambda, CI/CD, Docker, Monitoring)

---

## 🧱 Medallion Data Architecture (Bronze / Silver / Gold)

| Layer | Directory | Description |
|-------|-----------|-------------|
| 🟤 Bronze | `data/bronze/` | Raw data exactly as received |
| ⚪ Silver | `data/silver/` | Cleaned + validated + labeled |
| 🟡 Gold | `data/gold/` | Feature-optimized datasets used for ML training |

This architecture is widely used in modern Data Lakehouse / MLOps systems.

---
## 📂 Project Structure
```
project-root/
├── api/
│   └── application.py
├── config/
│   ├── config.yaml
│   └── params.yaml
├── data/
│   ├── bronze/            # Raw data (source dump)
│   ├── silver/            # Cleaned + validated data
│   └── gold/              # Feature-ready datasets for ML
├── docs/
│   ├── feature_01.md
│   └── feature_02.md
├── evaluation/
│   ├── evaluate_model_01.py
│   └── evaluate_model_02.py
├── examples/
│   └── basic_inference_example.py
├── notebooks/
│   └── trials.ipynb
├── project_cli/
│   ├── train.py
│   └── evaluate.py
├── src/
│   └── complaint_priority/
│       ├── __init__.py
│       ├── api/
│       │   ├── app.py
│       │   ├── routes.py
│       │   └── schemas.py
│       ├── cli/
│       │   ├── train_cli.py
│       │   └── predict_cli.py
│       ├── config/
│       │   ├── config_reader.py
│       │   └── constants.py
│       ├── data/
│       │   ├── download_dataset.py
│       │   ├── data_ingestion.py
│       │   ├── data_transformation.py
│       │   └── data_validation.py
│       ├── entity/
│       │   ├── data_entities.py
│       │   └── model_entities.py
│       ├── features/
│       │   ├── build_features.py
│       │   └── feature_selector.py
│       ├── infra/
│       │   ├── gcp/
│       │   └── sqs/
│       ├── models/
│       │   ├── train_model.py
│       │   ├── evaluate_model.py
│       │   ├── predict_model.py
│       │   └── registry.py
│       ├── network/
│       │   ├── approach_01.py
│       │   └── approach_02.py
│       ├── pipeline/
│       │   ├── train_pipeline.py
│       │   └── predict_pipeline.py
│       ├── utils/
│       │   ├── logging.py
│       │   ├── common.py
│       │   └── io_utils.py
│       └── visualization/
│           ├── visualize_data.py
│           └── visualize_model.py
├── tasks/
│   ├── download_data.sh
│   ├── lint.sh
│   └── run_training.sh
├── tests/
│   ├── test_api.py
│   ├── test_data.py
│   ├── test_features.py
│   └── test_models.py
├── training/
│   ├── experiments/
│   │   └── experiment_template.yaml
│   ├── prepare_experiment.py
│   ├── run_experiment.py
│   └── update_metadata.py
├── Dockerfile
├── requirements.txt
├── README.md
└── main.py
```
---

## 🧪 Testing Strategy

| Test Type | Location | Purpose |
|----------|----------|---------|
| Unit Tests | `tests/` | Validate individual components |
| Integration Tests | `api/tests` | End-to-end system behavior |

We aim for **>80% coverage** once complete.

---

## 🧩 Development Roadmap

| Stage | Component |
|-------|----------|
| ✔ Stage 1 | Project Setup + Structure |
| ✔ Stage 2 | Data ingestion → Bronze → Silver |
| 🔄  Stage 3 | Labeling → Gold |
| ⏳ Stage 4 | Feature engineering (text + tabular fusion) |
| ⏳ Stage 5 | Model training + evaluation pipeline |
| ⏳ Stage 6 | API + CLI Inference |
| ⏳ Stage 7 | Validation, Deployment Prep |
| ⏳ Stage 8 | Cloud Integration + CI/CD |

---

## 👤 Author

Venkata Dharaneswar Reddy  
📍 India  

---

🔥 Stay tuned — full pipeline coming soon!
