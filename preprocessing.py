import psycopg2
from typing import Dict, Any, List, Optional, Tuple
import json
import logging
from dataclasses import dataclass
from enum import Enum

class JoinType(Enum):
    HASH = "Hash Join"
    MERGE = "Merge Join"
    NESTED = "Nested Loop"

class ScanType(Enum):
    SEQUENTIAL = "Seq Scan"
    INDEX = "Index Scan"
    BITMAP = "Bitmap Scan"

@dataclass
class DatabaseConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

class QueryPreprocessor:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
        self._setup_logging()

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def connect(self) -> None:
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.dbname,
                user=self.config.user,
                password=self.config.password
            )
            self.logger.info("Successfully connected to database")
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {str(e)}")
            raise

    def disconnect(self) -> None:
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed")

    def get_table_metadata(self) -> Dict[str, List[str]]:
        """Retrieve metadata about tables and their columns"""
        if not self.connection:
            self.connect()

        query = """
        SELECT 
            table_name,
            array_agg(column_name::text) as columns
        FROM 
            information_schema.columns
        WHERE 
            table_schema = 'public'
        GROUP BY 
            table_name;
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                return {table: columns for table, columns in results}
        except Exception as e:
            self.logger.error(f"Error fetching table metadata: {str(e)}")
            raise

    def get_query_plan(self, sql: str, format_json: bool = True) -> Dict[str, Any]:
        """Get the query execution plan for a given SQL query"""
        if not self.connection:
            self.connect()

        try:
            with self.connection.cursor() as cursor:
                explain_query = f"EXPLAIN ({'FORMAT JSON' if format_json else ''}) {sql}"
                cursor.execute(explain_query)
                plan = cursor.fetchone()[0]
                return json.loads(plan[0]) if format_json else plan
        except Exception as e:
            self.logger.error(f"Error getting query plan: {str(e)}")
            raise

    def validate_sql(self, sql: str) -> Tuple[bool, Optional[str]]:
        """Validate SQL query syntax and structure"""
        if not sql.strip():
            return False, "Empty query"

        try:
            with self.connection.cursor() as cursor:
                # Parse query without executing
                cursor.execute(f"EXPLAIN {sql}")
                return True, None
        except psycopg2.Error as e:
            return False, str(e)
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"

    def analyze_query_complexity(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze query plan complexity and structure"""
        metrics = {
            'join_types': [],
            'scan_types': [],
            'total_cost': 0,
            'number_of_joins': 0,
            'tables_involved': set(),
        }

        def traverse_plan(node: Dict[str, Any]):
            node_type = node.get('Node Type', '')
            
            # Track join operations
            if 'Join' in node_type:
                metrics['join_types'].append(node_type)
                metrics['number_of_joins'] += 1

            # Track scan operations
            if 'Scan' in node_type:
                metrics['scan_types'].append(node_type)
                if 'Relation Name' in node:
                    metrics['tables_involved'].add(node['Relation Name'])

            # Track costs
            metrics['total_cost'] = max(
                metrics['total_cost'],
                node.get('Total Cost', 0)
            )

            # Recurse through child plans
            for child in node.get('Plans', []):
                traverse_plan(child)

        traverse_plan(plan['Plan'])
        metrics['tables_involved'] = list(metrics['tables_involved'])
        return metrics

    def extract_planner_hints(self, sql: str) -> List[str]:
        """Extract any existing planner hints from the SQL query"""
        # Implementation depends on your specific needs
        hints = []
        # Add logic to extract hints like /*+ IndexScan(table) */ etc.
        return hints

    def get_available_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """Get information about available indexes for a table"""
        query = """
        SELECT 
            i.indexname,
            array_agg(a.attname::text) as columns,
            ix.indisunique as is_unique
        FROM 
            pg_indexes i
            JOIN pg_class c ON c.relname = i.indexname
            JOIN pg_index ix ON ix.indexrelid = c.oid
            JOIN pg_attribute a ON a.attrelid = ix.indrelid 
            AND a.attnum = ANY(ix.indkey)
        WHERE 
            i.tablename = %s
        GROUP BY 
            i.indexname, ix.indisunique;
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (table_name,))
                results = cursor.fetchall()
                return [
                    {
                        'name': name,
                        'columns': columns,
                        'unique': is_unique
                    }
                    for name, columns, is_unique in results
                ]
        except Exception as e:
            self.logger.error(f"Error fetching index information: {str(e)}")
            return []