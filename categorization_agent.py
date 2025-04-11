"""
Agente di Categorizzazione per il sistema di estrazione e analisi di dati di gare d'appalto.

Questo modulo implementa l'Agente di Categorizzazione che si occupa di classificare
i bandi di gara in categorie e sottocategorie.
"""

import os
import json
import uuid
import logging
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
import openai
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configurazione del logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("categorization_agent.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("CategorizationAgent")

class CategorizationAgent:
    """
    Agente di Categorizzazione che utilizza l'API di OpenAI per classificare
    i bandi di gara in categorie e sottocategorie.
    """
    
    def __init__(self, api_key: str = None, config_path: str = "config.py"):
        """
        Inizializza l'Agente di Categorizzazione.
        
        Args:
            api_key (str, optional): Chiave API di OpenAI. Se None, usa la variabile d'ambiente OPENAI_API_KEY.
            config_path (str, optional): Percorso del file di configurazione. Default: "config.py".
        """
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("È necessario fornire una chiave API di OpenAI")
            
        self.client = openai.OpenAI(api_key=self.api_key)
        self.config_path = config_path
        self.config = self._load_config()
        
        # Definizione delle categorie
        self.categorie = {
            "Illuminazione": [
                "Pubblica", "Stradale", "Sportiva", "Architettonica", 
                "Votiva", "Interna", "Smart"
            ],
            "Videosorveglianza": [
                "Urbana", "Stradale", "Edifici", "Industriale", "Integrata"
            ],
            "Gallerie": [
                "Stradali", "Autostradali", "Ferroviarie", "Pedonali", 
                "Tecnologiche", "Servizi"
            ],
            "Edifici": [
                "Pubblici", "Scolastici", "Sanitari", "Residenziali", 
                "Commerciali", "Industriali"
            ],
            "Mobilità Elettrica": [
                "Colonnine", "Stazioni", "Infrastrutture", "Servizi"
            ],
            "Parcheggi": [
                "Gestione", "Automazione", "Parcometri", "Segnaletica"
            ],
            "Smart City": [
                "IoT", "Sensori", "Piattaforme", "Servizi"
            ],
            "Energia": [
                "Efficientamento", "Rinnovabili", "Gestione", "Monitoraggio"
            ]
        }
        
        # Tipi di appalto
        self.tipi_appalto = [
            "Affidamento", "Appalto", "Concessione", "Project Financing",
            "Servizio", "Fornitura", "Accordo Quadro", "Procedura Aperta",
            "Partenariato Pubblico Privato", "Gestione", "Noleggio"
        ]
        
        # Tipi di intervento
        self.tipi_intervento = [
            "Efficientamento Energetico", "Riqualificazione", "Manutenzione Ordinaria",
            "Manutenzione Straordinaria", "Adeguamento Normativo", "Gestione Impianti",
            "Rinnovo Impianti", "Installazione Nuovi Impianti", "Rifacimento",
            "Costruzione", "Sostituzione Componenti", "Restauro", "Potenziamento"
        ]
        
        # Statistiche
        self.stats = {
            "record_processati": 0,
            "record_categorizzati": 0,
            "errori": 0,
            "tempo_totale": 0,
            "token_utilizzati": 0
        }
        
        # Cache per evitare di rianalizzare testi identici
        self.cache = {}
        
        logger.info("Agente di Categorizzazione inizializzato")
    
    def _load_config(self) -> Dict:
        """
        Carica la configurazione dal file specificato.
        
        Returns:
            Dict: Configurazione del sistema.
        """
        try:
            # Importa il modulo di configurazione
            import importlib.util
            spec = importlib.util.spec_from_file_location("config", self.config_path)
            config_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(config_module)
            
            # Estrai le variabili dal modulo
            config = {name: getattr(config_module, name) for name in dir(config_module) 
                     if not name.startswith('__')}
            
            logger.info(f"Configurazione caricata con successo da {self.config_path}")
            return config
        except Exception as e:
            logger.error(f"Errore nel caricamento della configurazione: {str(e)}")
            # Configurazione di default
            return {
                "TEMP_DIR": "temp",
                "MAX_WORKERS": 10,
                "OPENAI_MODEL": "gpt-4o",
                "OPENAI_TEMPERATURE": 0.1,
                "CACHE_ENABLED": True
            }
    
    def categorize_record(self, record: Dict) -> Dict:
        """
        Categorizza un singolo record utilizzando l'API di OpenAI.
        
        Args:
            record (dict): Record da categorizzare
            
        Returns:
            dict: Record categorizzato
        """
        # Estrai il testo dell'oggetto
        oggetto = record.get("Oggetto", "")
        if not oggetto or len(oggetto.strip()) < 10:
            logger.warning(f"Oggetto troppo breve o mancante: {oggetto}")
            return record
        
        # Verifica se il testo è già nella cache
        cache_key = oggetto[:200]  # Usa i primi 200 caratteri come chiave
        if self.config.get("CACHE_ENABLED", True) and cache_key in self.cache:
            logger.info(f"Utilizzando risultato dalla cache per: {cache_key[:50]}...")
            categorization = self.cache[cache_key]
            
            # Copia le categorie nel record
            record_categorizzato = record.copy()
            for key, value in categorization.items():
                record_categorizzato[key] = value
                
            return record_categorizzato
        
        # Prepara il prompt per la categorizzazione
        prompt = f"""
        Analizza il seguente oggetto di un bando di gara ed estrai le categorie rilevanti.
        
        Oggetto del bando:
        ---
        {oggetto}
        ---
        
        Classifica questo bando nelle seguenti categorie:
        
        1. Categoria principale (una sola scelta tra):
           - Illuminazione
           - Videosorveglianza
           - Gallerie
           - Edifici
           - Mobilità Elettrica
           - Parcheggi
           - Smart City
           - Energia
           - Altro (se non rientra in nessuna delle precedenti)
        
        2. Sottocategoria (in base alla categoria principale):
           - Per Illuminazione: {", ".join(self.categorie["Illuminazione"])}
           - Per Videosorveglianza: {", ".join(self.categorie["Videosorveglianza"])}
           - Per Gallerie: {", ".join(self.categorie["Gallerie"])}
           - Per Edifici: {", ".join(self.categorie["Edifici"])}
           - Per Mobilità Elettrica: {", ".join(self.categorie["Mobilità Elettrica"])}
           - Per Parcheggi: {", ".join(self.categorie["Parcheggi"])}
           - Per Smart City: {", ".join(self.categorie["Smart City"])}
           - Per Energia: {", ".join(self.categorie["Energia"])}
        
        3. Tipo di appalto (una sola scelta tra):
           {", ".join(self.tipi_appalto)}
        
        4. Tipo di intervento (una sola scelta tra):
           {", ".join(self.tipi_intervento)}
        
        5. Rilevanza (valore numerico da 0 a 1):
           Quanto è rilevante questo bando per la categoria principale?
        
        6. Motivazione:
           Breve spiegazione del motivo per cui il bando è stato classificato in questa categoria.
        
        Rispondi in formato JSON con i seguenti campi:
        - Categoria: stringa con la categoria principale
        - Sottocategoria: stringa con la sottocategoria
        - TipoAppalto: stringa con il tipo di appalto
        - TipoIntervento: stringa con il tipo di intervento
        - Rilevanza: valore numerico da 0 a 1
        - Motivazione: stringa con la motivazione
        """
        
        try:
            start_time = time.time()
            response = self.client.chat.completions.create(
                model=self.config.get("OPENAI_MODEL", "gpt-4o"),
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": "Sei un assistente specializzato nella classificazione di bandi di gara. Rispondi solo in formato JSON valido."},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.config.get("OPENAI_TEMPERATURE", 0.1)
            )
            
            # Aggiorna il conteggio dei token
            self.stats["token_utilizzati"] += response.usage.total_tokens
            
            # Estrai e analizza la risposta JSON
            result_text = response.choices[0].message.content
            result = json.loads(result_text)
            
            # Aggiungi un timestamp e un ID
            result["timestamp_categorizzazione"] = datetime.now().isoformat()
            result["id_categorizzazione"] = str(uuid.uuid4())
            
            # Salva il risultato nella cache
            if self.config.get("CACHE_ENABLED", True):
                self.cache[cache_key] = result
            
            # Copia le categorie nel record
            record_categorizzato = record.copy()
            for key, value in result.items():
                record_categorizzato[key] = value
            
            logger.info(f"Record categorizzato: {result['Categoria']} - {result['Sottocategoria']} (Rilevanza: {result['Rilevanza']})")
            return record_categorizzato
            
        except Exception as e:
            logger.error(f"Errore nella categorizzazione del record: {str(e)}")
            self.stats["errori"] += 1
            return record
    
    def run(self, records: List[Dict]) -> List[Dict]:
        """
        Esegue la categorizzazione di una lista di record.
        
        Args:
            records (list): Lista di record da categorizzare
            
        Returns:
            list: Lista di record categorizzati
        """
        logger.info(f"Avvio categorizzazione di {len(records)} record")
        
        # Timestamp di inizio per calcolare il tempo totale
        start_time = time.time()
        
        if not records:
            logger.warning("Nessun record da categorizzare")
            return []
        
        # Risultati
        categorized_records = []
        
        # Calcola il numero ottimale di worker in base al numero di record
        max_workers = min(self.config.get("MAX_WORKERS", 10), max(1, len(records)))
        
        # Categorizza i record in parallelo
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Crea un dizionario di future per tenere traccia dei record
            futures = {}
            for record in records:
                future = executor.submit(self.categorize_record, record)
                futures[future] = record
            
            # Processa i risultati man mano che sono disponibili
            completed = 0
            for future in as_completed(futures):
                record = futures[future]
                try:
                    categorized_record = future.result()
                    categorized_records.append(categorized_record)
                    
                    completed += 1
                    self.stats["record_processati"] += 1
                    
                    # Se il record ha una categoria, incrementa il contatore
                    if "Categoria" in categorized_record and categorized_record["Categoria"] != "Altro":
                        self.stats["record_categorizzati"] += 1
                    
                    # Log di avanzamento ogni 10 record completati o alla fine
                    if completed % 10 == 0 or completed == len(records):
                        logger.info(f"Progresso: {completed}/{len(records)} record elaborati")
                        
                except Exception as e:
                    logger.error(f"Errore nell'elaborazione del record: {e}")
                    self.stats["errori"] += 1
                    # Aggiungiamo il record originale per non perdere dati
                    categorized_records.append(record)
                    continue
        
        # Calcola e registra il tempo totale
        self.stats["tempo_totale"] = time.time() - start_time
        
        logger.info(f"Categorizzazione completata in {self.stats['tempo_totale']:.2f} secondi")
        logger.info(f"Statistiche finali: Record processati: {self.stats['record_processati']}, "
                   f"Record categorizzati: {self.stats['record_categorizzati']}, Errori: {self.stats['errori']}, "
                   f"Token utilizzati: {self.stats['token_utilizzati']}")
        
        return categorized_records


class CategorizationAgentRunner:
    """
    Classe per l'esecuzione dell'Agente di Categorizzazione.
    """
    
    @staticmethod
    def run(records: List[Dict]) -> List[Dict]:
        """
        Esegue la categorizzazione dei record.
        
        Args:
            records (list): Lista di record da categorizzare
            
        Returns:
            list: Lista di record categorizzati
        """
        try:
            # Ottieni la chiave API da config o da variabile d'ambiente
            api_key = os.environ.get("OPENAI_API_KEY")
            
            if not api_key:
                logger.error("Chiave API di OpenAI non trovata. Imposta la variabile d'ambiente OPENAI_API_KEY.")
                return records
            
            # Inizializza e esegui l'agente di categorizzazione
            categorizer = CategorizationAgent(api_key=api_key)
            categorized_records = categorizer.run(records)
            
            return categorized_records
            
        except Exception as e:
            logger.error(f"Errore durante l'esecuzione dell'agente di categorizzazione: {str(e)}")
            return records

# Esecuzione diretta dello script
if __name__ == "__main__":
    import sys
    import json
    
    # Se viene fornito un file JSON come argomento, categorizza i record in quel file
    if len(sys.argv) > 1:
        json_file = sys.argv[1]
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                records = json.load(f)
            
            categorized_records = CategorizationAgentRunner.run(records)
            
            # Salva i record categorizzati
            output_file = json_file.replace('.json', '_categorized.json')
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(categorized_records, f, ensure_ascii=False, indent=2)
                
            print(f"Categorizzati {len(categorized_records)} record. Risultati salvati in {output_file}")
            
        except Exception as e:
            print(f"Errore: {e}")
    else:
        print("Specificare un file JSON come argomento")
