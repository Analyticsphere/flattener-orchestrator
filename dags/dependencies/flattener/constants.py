import os

PROCESSOR_ENDPOINT = os.environ.get("FLATTENER_PROCESSOR_ENDPOINT")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_FLATTENED_BUCKET = os.environ.get("GCS_FLATTENED_BUCKET")

BQ_RAW_DATASET = "Connect"
BQ_FLATTENED_DATASET = "FlatConnect"

FIRESTORE_REFRESH_TOPIC = "schedule-firestore-backup"

RAW_TABLES = [
    "bioSurvey_v1",
    "biospecimen",
    "birthdayCard",
    #"boxes", # This flattened table is created by scheduled query FlatConnect.boxes_JP
    "cancerOccurrence",
    "cancerScreeningHistorySurvey",
    "clinicalBioSurvey_v1",
    "covid19Survey_v1",
    "experience2024",
    "kitAssembly",
    "menstrualSurvey_v1",
    "module1_v1",
    "module1_v2",
    "module2_v1",
    "module2_v2",
    "module3_v1",
    "module4_v1",
    "mouthwash_v1",
    "notifications",
    "participants",
    "promis_v1",
    "sendgridTracking",
    "preference2025"
]