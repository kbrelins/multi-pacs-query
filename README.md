# multi_pacs_query17_exclude

**Skrypt do wykonywania zapytań DICOM C-FIND na serwerach PACS z obsługą filtrowania modalności, wykluczania wybranych badań i zapisem wyników do CSV.**

## Opis (PL)
Skrypt służy do wykonywania zapytań C-FIND (DICOM) na serwerach PACS w celu pobrania informacji o badaniach i seriach obrazów.  
Obsługuje równoległe zapytania do wielu serwerów, filtrowanie badań wg modalności (`--modality`) oraz wykluczanie modalności (`--exclude`).  
Wyniki są zapisywane w pliku CSV zawierającym listę badań oraz brakujących serii.

### Funkcje główne
- **load_servers** – ładuje konfigurację serwerów z pliku `.cfg`
- **query_server** – wykonuje zapytanie C-FIND na poziomie STUDY
- **query_study_series** – pobiera listę serii (SERIES level)
- **query_server_with_4h_blocks** – dzieli zapytania na 4-godzinne bloki przy dużej liczbie wyników
- **filter_study** – filtruje badania wg modalności
- **process_server** – pobiera badania i serie z serwera, zwraca w ustrukturyzowanej formie
- **main** – zarządza logiką działania skryptu, zapisuje dane do CSV

### Parametry
- `--start_date YYYYMMDD` – data początkowa
- `--end_date YYYYMMDD` – data końcowa
- `--modality` – lista modalności do filtrowania (domyślnie NONE = wszystkie)
- `--exclude` – lista modalności do wykluczenia
- `--cfg` – plik konfiguracyjny z listą serwerów PACS
- `--output` – plik wynikowy CSV
- `--aet` – lokalny AET (Application Entity Title)

### Wymagania
- Python **3.9 lub nowszy**
- Biblioteki:
  - `pynetdicom`
  - `pydicom`
  - `argparse` (wbudowane w Python)

### Instalacja zależności
```bash
pip install pynetdicom pydicom
```

---

**Python script for performing DICOM C-FIND queries on PACS servers with modality filtering, exclusion options, and CSV results export.**

## Description (EN)
The script performs C-FIND (DICOM) queries on PACS servers to retrieve information about studies and image series.  
It supports parallel queries to multiple servers, filtering studies by modality (`--modality`), and excluding specific modalities (`--exclude`).  
The results are saved in a CSV file containing a list of studies and missing series.

### Main functions
- **load_servers** – load server configuration from `.cfg` file
- **query_server** – perform C-FIND query at STUDY level
- **query_study_series** – retrieve series list (SERIES level)
- **query_server_with_4h_blocks** – split queries into 4-hour blocks if too many results
- **filter_study** – filter studies by modalities
- **process_server** – fetch studies and series from a server and return structured data
- **main** – orchestrates script execution and writes results to CSV

### Parameters
- `--start_date YYYYMMDD` – start date
- `--end_date YYYYMMDD` – end date
- `--modality` – list of modalities to include (default NONE = all)
- `--exclude` – list of modalities to exclude
- `--cfg` – config file with PACS servers list
- `--output` – output CSV file
- `--aet` – local AET (Application Entity Title)

### Requirements
- Python **3.9 or newer**
- Libraries:
  - `pynetdicom`
  - `pydicom`
  - `argparse` (built into Python)

### Installing dependencies
```bash
pip install pynetdicom pydicom
```

