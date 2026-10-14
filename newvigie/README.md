# Transparence — pipeline Data Engineering et tableau de bord

Ce projet découvre récursivement les rapports CSV d'un bucket Google Cloud
Storage, harmonise leurs colonnes et leurs types, puis consolide les lignes dans
BigQuery. L'application Streamlit lit directement la table consolidée.

## Arborescence

```text
.
├── .streamlit/
│   └── secrets.toml.example
├── app.py
├── config.yaml
├── pipeline/
│   ├── __init__.py
│   ├── extractor.py
│   ├── loader.py
│   ├── settings.py
│   └── transformer.py
├── requirements.txt
└── tests/
    └── test_transformer.py
```

## Prérequis GCP

1. Activez les API **Cloud Storage** et **BigQuery** dans le projet GCP.
2. Le compte qui exécute le pipeline a besoin de lire les objets du bucket
   (par exemple `roles/storage.objectViewer`) et de créer/charger les tables du
   dataset (par exemple `roles/bigquery.jobUser` et
   `roles/bigquery.dataEditor`). La création du dataset nécessite aussi la
   permission `bigquery.datasets.create` au niveau du projet.
3. Le compte de service utilisé par Streamlit Cloud doit disposer au minimum
   des permissions BigQuery de lecture sur le dataset. N'accordez que les rôles
   nécessaires et évitez les clés de compte de service lorsque votre
   environnement permet une fédération d'identité.

## Installation locale (Windows / VS Code)

Depuis le dossier du projet, créez un environnement virtuel, activez-le puis
installez les dépendances :

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Renseignez les valeurs non secrètes dans `config.yaml` : `project_id`,
`dataset_id`, `table_id`, `bucket_name`, `region` et, éventuellement, `prefix`.
Le préfixe limite la découverte à un chemin du bucket ; une chaîne vide parcourt
tous ses objets. Les variables `GCP_PROJECT_ID`, `BIGQUERY_DATASET`,
`BIGQUERY_TABLE`, `GCS_BUCKET_NAME`, `GCP_REGION` et `GCS_PREFIX` peuvent
remplacer les valeurs du YAML.

Les identifiants ne sont jamais enregistrés dans `config.yaml`. En local, la
méthode recommandée est l'authentification ADC :

```powershell
gcloud auth application-default login
```

Le compte ADC doit posséder les permissions GCP décrites plus haut.

## Exécution

Depuis la racine du projet :

```powershell
python -m pipeline.loader
streamlit run app.py
```

Le pipeline :

1. parcourt tous les sous-dossiers du bucket et retient les extensions `.csv`
   sans distinction de casse ;
2. détecte le séparateur et tente les encodages UTF-8, Windows-1252 et Latin-1 ;
3. normalise les noms de colonnes (accents, casse, espaces et alias connus),
   fait l'union des schémas et remplace les valeurs numériques sales illisibles
   par `NULL` ;
4. supprime les doublons stricts puis, quand `date` et `organisme` sont
   présents, les doublons métier (avec `type_demande` si disponible). Les
   enregistrements à clé incomplète sont conservés. Les suppressions sont
   journalisées ;
5. ajoute à chaque ligne `source_file` (URI GCS complète) et `ingested_at`
   (horodatage UTC) ;
6. crée le dataset s'il manque, charge un staging BigQuery propre au run et
   met à jour la table consolidée. Les colonnes ajoutées par de nouveaux
   rapports sont ajoutées au schéma final. Les staging sont conservés pour
   faciliter le diagnostic et l'audit.

La transformation utilise un DataFrame consolidé en mémoire avant le
chargement BigQuery. Pour des volumes dépassant la mémoire disponible, adaptez
le traitement en ingestion par lots / fichiers ou utilisez des tables externes
   BigLake avant le dédoublonnage consolidé. Exécutez un seul pipeline à la fois
   pour éviter que deux consolidations simultanées ne se remplacent mutuellement.

## Secrets Streamlit Cloud

Dans **Streamlit Community Cloud > App > Settings > Secrets**, définissez les
paramètres non sensibles dans une section `[gcp]` et, si nécessaire, les champs
JSON du compte de service dans `[gcp_service_account]`. Le modèle est fourni
dans `.streamlit/secrets.toml.example`. Le fichier local
`.streamlit/secrets.toml` est ignoré par Git et ne doit jamais être commité.
Les secrets `[gcp]` remplacent ceux de `config.yaml`. Si aucun compte de service
n'est fourni, l'application utilise ADC.

Exemple de paramètres (remplacez les valeurs d'exemple) :

```toml
[gcp]
project_id = "mon-projet"
dataset_id = "transparency"
table_id = "reports_consolides"
bucket_name = "mon-bucket"
region = "europe-west1"
prefix = ""
```

Pour un compte de service, collez ses champs JSON dans la section
`[gcp_service_account]` de l'interface Secrets Streamlit. Ne placez jamais la
clé privée dans le dépôt Git, dans `config.yaml` ou dans un artefact de build.

## Déploiement via GitHub et Streamlit Cloud

1. Poussez le projet vers un dépôt GitHub privé ou public, sans
   `.streamlit/secrets.toml`, clé JSON ni autre identifiant.
2. Créez une application sur Streamlit Community Cloud en sélectionnant le
   dépôt, la branche et `app.py`.
3. Configurez les secrets décrits ci-dessus et vérifiez les droits BigQuery du
   compte de service.
4. Exécutez le pipeline dans un environnement autorisé à lire GCS et à écrire
   BigQuery avant d'ouvrir le tableau de bord. Streamlit Cloud n'exécute pas
   automatiquement le pipeline.

L'interface propose des filtres globaux par source, organisme et période
(année lorsque seule une colonne d'année est disponible), des indicateurs
calculés dans BigQuery, des graphiques adaptés aux champs numériques /
temporels et un aperçu filtré. Le téléchargement CSV, déclenché explicitement,
contient toutes les lignes qui correspondent aux filtres ; son résultat est
chargé en mémoire par l'application.

## Tests

Les tests unitaires de transformation utilisent `unittest` :

```powershell
python -m unittest discover -s tests -v
```
