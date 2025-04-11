"""
Implementazione dell'agente di persistenza dati utilizzando l'SDK ufficiale di OpenAI Agents.

Questo modulo implementa l'agente specializzato per la persistenza dei dati in database
e l'esportazione in vari formati.
"""

import os
import logging
import json
import sqlite3
import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime
import asyncio
from pydantic import BaseModel

from agents import Agent, Runner, Context, tool

# Configurazione del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("persistence_agent.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("PersistenceAgent")

# Definizione dell'agente di persistenza dati
persistence_agent = Agent(
    name="Data Persistence",
    handoff_description="Specialist agent for data persistence in database and export to various formats",
    instructions="""
    Store data in a SQLite database and export it to various formats such as Excel, CSV, and JSON.
    Create and manage database tables, handle data insertion and updates, and generate reports.
    
    Follow these steps:
    1. Create or connect to a SQLite database
    2. Create or update database tables
    3. Insert or update data in the database
    4. Export data to various formats
    5. Generate reports
    
    Focus on these aspects:
    - Proper database schema design
    - Efficient data insertion and updates
    - Comprehensive data export
    - Detailed reporting
    """
)

# Modelli di dati per l'output strutturato
class TableInfo(BaseModel):
    """Modello per le informazioni su una tabella."""
    name: str
    columns: List[str]
    primary_key: Optional[str] = None
    foreign_keys: Optional[List[Dict[str, str]]] = None

class DatabaseSchema(BaseModel):
    """Modello per lo schema del database."""
    tables: List[TableInfo]
    relationships: Optional[List[Dict[str, str]]] = None

class ExportResult(BaseModel):
    """Modello per il risultato dell'esportazione."""
    format: str
    file_path: str
    record_count: int
    execution_time: float
    timestamp: str

class PersistenceResult(BaseModel):
    """Modello per il risultato della persistenza."""
    database_path: str
    tables_created: List[str]
    records_inserted: Dict[str, int]
    exports: List[ExportResult]
    execution_time: float
    timestamp: str

# Schema del database
DATABASE_SCHEMA = {
    "tenders": {
        "columns": [
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "cig TEXT",
            "title TEXT",
            "description TEXT",
            "contracting_authority TEXT",
            "publication_date TEXT",
            "deadline TEXT",
            "amount REAL",
            "status TEXT",
            "procurement_method TEXT",
            "source TEXT"
        ],
        "indices": [
            "CREATE INDEX IF NOT EXISTS idx_tenders_cig ON tenders(cig)"
        ]
    },
    "categories": {
        "columns": [
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "name TEXT UNIQUE",
            "description TEXT"
        ]
    },
    "subcategories": {
        "columns": [
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "category_id INTEGER",
            "name TEXT",
            "description TEXT",
            "FOREIGN KEY (category_id) REFERENCES categories(id)"
        ],
        "indices": [
            "CREATE INDEX IF NOT EXISTS idx_subcategories_category_id ON subcategories(category_id)"
        ]
    },
    "tender_categories": {
        "columns": [
            "tender_id INTEGER",
            "category_id INTEGER",
            "subcategory_id INTEGER",
            "confidence REAL",
            "PRIMARY KEY (tender_id, category_id, subcategory_id)",
            "FOREIGN KEY (tender_id) REFERENCES tenders(id)",
            "FOREIGN KEY (category_id) REFERENCES categories(id)",
            "FOREIGN KEY (subcategory_id) REFERENCES subcategories(id)"
        ]
    },
    "awards": {
        "columns": [
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "tender_id INTEGER",
            "award_date TEXT",
            "award_amount REAL",
            "status TEXT",
            "FOREIGN KEY (tender_id) REFERENCES tenders(id)"
        ],
        "indices": [
            "CREATE INDEX IF NOT EXISTS idx_awards_tender_id ON awards(tender_id)"
        ]
    },
    "suppliers": {
        "columns": [
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "name TEXT UNIQUE",
            "identifier TEXT"
        ]
    },
    "award_suppliers": {
        "columns": [
            "award_id INTEGER",
            "supplier_id INTEGER",
            "PRIMARY KEY (award_id, supplier_id)",
            "FOREIGN KEY (award_id) REFERENCES awards(id)",
            "FOREIGN KEY (supplier_id) REFERENCES suppliers(id)"
        ]
    },
    "locations": {
        "columns": [
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "tender_id INTEGER",
            "region TEXT",
            "province TEXT",
            "municipality TEXT",
            "FOREIGN KEY (tender_id) REFERENCES tenders(id)"
        ],
        "indices": [
            "CREATE INDEX IF NOT EXISTS idx_locations_tender_id ON locations(tender_id)"
        ]
    },
    "lots": {
        "columns": [
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "tender_id INTEGER",
            "lot_number TEXT",
            "title TEXT",
            "description TEXT",
            "amount REAL",
            "FOREIGN KEY (tender_id) REFERENCES tenders(id)"
        ],
        "indices": [
            "CREATE INDEX IF NOT EXISTS idx_lots_tender_id ON lots(tender_id)"
        ]
    },
    "semantic_analysis": {
        "columns": [
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "tender_id INTEGER",
            "keywords TEXT",
            "summary TEXT",
            "sentiment REAL",
            "FOREIGN KEY (tender_id) REFERENCES tenders(id)"
        ],
        "indices": [
            "CREATE INDEX IF NOT EXISTS idx_semantic_analysis_tender_id ON semantic_analysis(tender_id)"
        ]
    }
}

# Strumenti (tools) per l'agente
@persistence_agent.tool
def create_database(database_path: str) -> Dict[str, Any]:
    """
    Crea un database SQLite.
    
    Args:
        database_path: Percorso del database
        
    Returns:
        Risultato della creazione
    """
    logger.info(f"Creazione del database: {database_path}")
    
    try:
        # Crea la directory se non esiste
        database_dir = os.path.dirname(database_path)
        if database_dir and not os.path.exists(database_dir):
            os.makedirs(database_dir)
        
        # Connessione al database
        conn = sqlite3.connect(database_path)
        conn.close()
        
        logger.info(f"Database creato con successo: {database_path}")
        return {
            "success": True,
            "database_path": database_path
        }
    except Exception as e:
        logger.error(f"Errore durante la creazione del database: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

@persistence_agent.tool
def create_tables(database_path: str) -> Dict[str, Any]:
    """
    Crea le tabelle nel database.
    
    Args:
        database_path: Percorso del database
        
    Returns:
        Risultato della creazione delle tabelle
    """
    logger.info(f"Creazione delle tabelle nel database: {database_path}")
    
    try:
        # Connessione al database
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()
        
        tables_created = []
        
        # Crea le tabelle
        for table_name, table_info in DATABASE_SCHEMA.items():
            # Crea la tabella
            columns = ", ".join(table_info["columns"])
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
            cursor.execute(create_table_sql)
            
            # Crea gli indici
            if "indices" in table_info:
                for index_sql in table_info["indices"]:
                    cursor.execute(index_sql)
            
            tables_created.append(table_name)
        
        # Commit delle modifiche
        conn.commit()
        conn.close()
        
        logger.info(f"Tabelle create con successo: {', '.join(tables_created)}")
        return {
            "success": True,
            "tables_created": tables_created
        }
    except Exception as e:
        logger.error(f"Errore durante la creazione delle tabelle: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

@persistence_agent.tool
def get_database_schema(database_path: str) -> Dict[str, Any]:
    """
    Ottiene lo schema del database.
    
    Args:
        database_path: Percorso del database
        
    Returns:
        Schema del database
    """
    logger.info(f"Ottenimento dello schema del database: {database_path}")
    
    try:
        # Connessione al database
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()
        
        # Ottieni l'elenco delle tabelle
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        schema = []
        relationships = []
        
        for table in tables:
            table_name = table[0]
            
            # Salta le tabelle di sistema
            if table_name.startswith("sqlite_"):
                continue
            
            # Ottieni le informazioni sulle colonne
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns_info = cursor.fetchall()
            
            columns = []
            primary_key = None
            
            for column_info in columns_info:
                column_name = column_info[1]
                column_type = column_info[2]
                is_pk = column_info[5]
                
                columns.append(f"{column_name} {column_type}")
                
                if is_pk:
                    primary_key = column_name
            
            # Ottieni le informazioni sulle chiavi esterne
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            foreign_keys_info = cursor.fetchall()
            
            foreign_keys = []
            
            for fk_info in foreign_keys_info:
                ref_table = fk_info[2]
                from_column = fk_info[3]
                to_column = fk_info[4]
                
                foreign_keys.append({
                    "column": from_column,
                    "references": {
                        "table": ref_table,
                        "column": to_column
                    }
                })
                
                relationships.append({
                    "from_table": table_name,
                    "from_column": from_column,
                    "to_table": ref_table,
                    "to_column": to_column
                })
            
            # Aggiungi le informazioni sulla tabella
            schema.append(TableInfo(
                name=table_name,
                columns=columns,
                primary_key=primary_key,
                foreign_keys=foreign_keys
            ).dict())
        
        conn.close()
        
        # Crea lo schema del database
        database_schema = DatabaseSchema(
            tables=schema,
            relationships=relationships
        )
        
        logger.info(f"Schema del database ottenuto con successo: {len(schema)} tabelle")
        return {
            "success": True,
            "schema": database_schema.dict()
        }
    except Exception as e:
        logger.error(f"Errore durante l'ottenimento dello schema del database: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

@persistence_agent.tool
def insert_data(database_path: str, table_name: str, data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Inserisce dati in una tabella.
    
    Args:
        database_path: Percorso del database
        table_name: Nome della tabella
        data: Dati da inserire
        
    Returns:
        Risultato dell'inserimento
    """
    logger.info(f"Inserimento di dati nella tabella {table_name}: {len(data)} record")
    
    try:
        # Connessione al database
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()
        
        # Verifica che la tabella esista
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not cursor.fetchone():
            conn.close()
            logger.error(f"La tabella {table_name} non esiste")
            return {
                "success": False,
                "error": f"La tabella {table_name} non esiste"
            }
        
        # Ottieni le informazioni sulle colonne
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        
        columns = [column_info[1] for column_info in columns_info]
        
        # Rimuovi la colonna ID se è autoincrement
        if "id" in columns and "INTEGER PRIMARY KEY AUTOINCREMENT" in DATABASE_SCHEMA.get(table_name, {}).get("columns", []):
            columns.remove("id")
        
        # Inserisci i dati
        inserted_count = 0
        
        for record in data:
            # Filtra le chiavi che corrispondono alle colonne
            filtered_record = {k: v for k, v in record.items() if k in columns}
            
            # Prepara la query
            placeholders = ", ".join(["?"] * len(filtered_record))
            columns_str = ", ".join(filtered_record.keys())
            
            # Esegui la query
            cursor.execute(
                f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})",
                list(filtered_record.values())
            )
            
            inserted_count += 1
        
        # Commit delle modifiche
        conn.commit()
        conn.close()
        
        logger.info(f"Dati inseriti con successo nella tabella {table_name}: {inserted_count} record")
        return {
            "success": True,
            "table_name": table_name,
            "inserted_count": inserted_count
        }
    except Exception as e:
        logger.error(f"Errore durante l'inserimento dei dati nella tabella {table_name}: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

@persistence_agent.tool
def execute_query(database_path: str, query: str) -> Dict[str, Any]:
    """
    Esegue una query SQL.
    
    Args:
        database_path: Percorso del database
        query: Query SQL da eseguire
        
    Returns:
        Risultato della query
    """
    logger.info(f"Esecuzione della query: {query}")
    
    try:
        # Connessione al database
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()
        
        # Esegui la query
        cursor.execute(query)
        
        # Se è una query SELECT, ottieni i risultati
        if query.strip().upper().startswith("SELECT"):
            # Ottieni i nomi delle colonne
            columns = [description[0] for description in cursor.description]
            
            # Ottieni i risultati
            rows = cursor.fetchall()
            
            # Converti i risultati in una lista di dizionari
            results = []
            for row in rows:
                result = {}
                for i, column in enumerate(columns):
                    result[column] = row[i]
                results.append(result)
            
            conn.close()
            
            logger.info(f"Query eseguita con successo: {len(results)} risultati")
            return {
                "success": True,
                "results": results
            }
        else:
            # Commit delle modifiche
            conn.commit()
            
            # Ottieni il numero di righe modificate
            affected_rows = cursor.rowcount
            
            conn.close()
            
            logger.info(f"Query eseguita con successo: {affected_rows} righe modificate")
            return {
                "success": True,
                "affected_rows": affected_rows
            }
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione della query: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

@persistence_agent.tool
def export_to_excel(database_path: str, query: str, output_file: str) -> Dict[str, Any]:
    """
    Esporta i risultati di una query in un file Excel.
    
    Args:
        database_path: Percorso del database
        query: Query SQL da eseguire
        output_file: Percorso del file Excel di output
        
    Returns:
        Risultato dell'esportazione
    """
    logger.info(f"Esportazione in Excel: {output_file}")
    
    start_time = datetime.now()
    
    try:
        # Connessione al database
        conn = sqlite3.connect(database_path)
        
        # Esegui la query e ottieni i risultati come DataFrame
        df = pd.read_sql_query(query, conn)
        
        # Chiudi la connessione
        conn.close()
        
        # Crea la directory di output se non esiste
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Esporta in Excel
        df.to_excel(output_file, index=False)
        
        # Crea il risultato dell'esportazione
        result = ExportResult(
            format="excel",
            file_path=output_file,
            record_count=len(df),
            execution_time=(datetime.now() - start_time).total_seconds(),
            timestamp=datetime.now().isoformat()
        )
        
        logger.info(f"Esportazione in Excel completata: {len(df)} record")
        return {
            "success": True,
            "result": result.dict()
        }
    except Exception as e:
        logger.error(f"Errore durante l'esportazione in Excel: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

@persistence_agent.tool
def export_to_csv(database_path: str, query: str, output_file: str) -> Dict[str, Any]:
    """
    Esporta i risultati di una query in un file CSV.
    
    Args:
        database_path: Percorso del database
        query: Query SQL da eseguire
        output_file: Percorso del file CSV di output
        
    Returns:
        Risultato dell'esportazione
    """
    logger.info(f"Esportazione in CSV: {output_file}")
    
    start_time = datetime.now()
    
    try:
        # Connessione al database
        conn = sqlite3.connect(database_path)
        
        # Esegui la query e ottieni i risultati come DataFrame
        df = pd.read_sql_query(query, conn)
        
        # Chiudi la connessione
        conn.close()
        
        # Crea la directory di output se non esiste
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Esporta in CSV
        df.to_csv(output_file, index=False)
        
        # Crea il risultato dell'esportazione
        result = ExportResult(
            format="csv",
            file_path=output_file,
            record_count=len(df),
            execution_time=(datetime.now() - start_time).total_seconds(),
            timestamp=datetime.now().isoformat()
        )
        
        logger.info(f"Esportazione in CSV completata: {len(df)} record")
        return {
            "success": True,
            "result": result.dict()
        }
    except Exception as e:
        logger.error(f"Errore durante l'esportazione in CSV: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

@persistence_agent.tool
def export_to_json(database_path: str, query: str, output_file: str) -> Dict[str, Any]:
    """
    Esporta i risultati di una query in un file JSON.
    
    Args:
        database_path: Percorso del database
        query: Query SQL da eseguire
        output_file: Percorso del file JSON di output
        
    Returns:
        Risultato dell'esportazione
    """
    logger.info(f"Esportazione in JSON: {output_file}")
    
    start_time = datetime.now()
    
    try:
        # Connessione al database
        conn = sqlite3.connect(database_path)
        
        # Esegui la query e ottieni i risultati come DataFrame
        df = pd.read_sql_query(query, conn)
        
        # Chiudi la connessione
        conn.close()
        
        # Crea la directory di output se non esiste
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Converti il DataFrame in una lista di dizionari
        records = df.to_dict(orient="records")
        
        # Esporta in JSON
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        
        # Crea il risultato dell'esportazione
        result = ExportResult(
            format="json",
            file_path=output_file,
            record_count=len(records),
            execution_time=(datetime.now() - start_time).total_seconds(),
            timestamp=datetime.now().isoformat()
        )
        
        logger.info(f"Esportazione in JSON completata: {len(records)} record")
        return {
            "success": True,
            "result": result.dict()
        }
    except Exception as e:
        logger.error(f"Errore durante l'esportazione in JSON: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

@persistence_agent.tool
def generate_persistence_result(database_path: str, tables_created: List[str], records_inserted: Dict[str, int], exports: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Genera il risultato della persistenza.
    
    Args:
        database_path: Percorso del database
        tables_created: Lista delle tabelle create
        records_inserted: Dizionario con il numero di record inseriti per tabella
        exports: Lista dei risultati dell'esportazione
        
    Returns:
        Risultato della persistenza
    """
    logger.info("Generazione del risultato della persistenza")
    
    start_time = datetime.now()
    
    # Crea il risultato della persistenza
    result = PersistenceResult(
        database_path=database_path,
        tables_created=tables_created,
        records_inserted=records_inserted,
        exports=exports,
        execution_time=(datetime.now() - start_time).total_seconds(),
        timestamp=datetime.now().isoformat()
    )
    
    logger.info("Risultato della persistenza generato")
    return result.dict()

# Funzione principale per l'esecuzione dell'agente
async def run_data_persistence(database_path: str, data: Dict[str, List[Dict[str, Any]]], export_formats: List[str] = ["excel", "json"]) -> Any:
    """
    Esegue la persistenza dei dati.
    
    Args:
        database_path: Percorso del database
        data: Dati da inserire
        export_formats: Formati di esportazione
        
    Returns:
        Risultato dell'esecuzione dell'agente
    """
    logger.info(f"Avvio della persistenza dei dati: database_path={database_path}")
    
    # Configurazione del contesto
    context = Context()
    
    # Prepara l'input per l'agente
    input_data = f"""
    Persisti i dati nel database e esportali in vari formati.
    
    Database: {database_path}
    Formati di esportazione: {', '.join(export_formats)}
    
    Segui questi passaggi:
    1. Crea il database se non esiste
    2. Crea le tabelle se non esistono
    3. Inserisci i dati nelle tabelle
    4. Esporta i dati nei formati richiesti
    5. Genera un report completo
    """
    
    # Esecuzione dell'agente
    result = await Runner.run(
        persistence_agent,
        input_data,
        context=context
    )
    
    logger.info("Persistenza dei dati completata")
    return result

# Punto di ingresso per l'esecuzione da linea di comando
if __name__ == "__main__":
    import argparse
    
    # Parsing degli argomenti da linea di comando
    parser = argparse.ArgumentParser(description="Persistenza dei dati")
    parser.add_argument("--database", default="output/gare_appalto.db",
                        help="Percorso del database")
    parser.add_argument("--data-file", required=True,
                        help="Percorso del file JSON contenente i dati da inserire")
    parser.add_argument("--export-formats", default="excel,json",
                        help="Formati di esportazione (separati da virgola)")
    args = parser.parse_args()
    
    # Verifica che la chiave API di OpenAI sia impostata
    if not os.environ.get("OPENAI_API_KEY"):
        print("ATTENZIONE: La variabile d'ambiente OPENAI_API_KEY non è impostata.")
        print("Per utilizzare il sistema, esegui:")
        print("export OPENAI_API_KEY='la_tua_chiave_api'")
        exit(1)
    
    # Carica i dati
    with open(args.data_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Esecuzione dell'agente
    result = asyncio.run(run_data_persistence(args.database, data, args.export_formats.split(",")))
    
    # Stampa dei risultati
    print("\nRisultato dell'esecuzione:")
    print(result.final_output)
