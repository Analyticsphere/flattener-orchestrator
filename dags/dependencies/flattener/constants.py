#ROCESSOR_ENDPOINT = "https://ccc-flattener-eaf-dev-1061430463455.us-central1.run.app"
PROCESSOR_ENDPOINT= "https://ccc-flattener-155089172944.us-central1.run.app"

#BQ_PROJECT_ID = "nih-nci-dceg-connect-dev"
BQ_PROJECT_ID = "nih-nci-dceg-connect-prod-6d04"
BQ_RAW_DATASET = "Connect"
BQ_FLATTENED_DATASET = "FlatConnect"
#GCS_FLATTENED_BUCKET = "flattener_tmp_dev"
GCS_FLATTENED_BUCKET = "flattener_tmp"

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
    "sendgridTracking"
]