import sys
import logging
from typing import Optional
from interface import QueryPlanAnalyzer
from preprocessing import QueryPreprocessor, DatabaseConfig
from whatif import QueryPlanModifier

class QueryPlanAnalysisSystem:
    def __init__(self):
        self._setup_logging()
        self.preprocessor = None
        self.modifier = None
        self.gui = None

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('query_analysis.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def initialize(self):
        """Initialize the system components"""
        try:
            # Create GUI first - it will handle login
            self.gui = QueryPlanAnalyzer()
            
            # Set up callback for after successful login
            self.gui.after_login_callback = self.post_login_setup
            
            self.logger.info("GUI initialized successfully")

        except Exception as e:
            self.logger.error(f"Error during system initialization: {str(e)}")
            raise

    def post_login_setup(self, connection):
        """Setup components after successful database connection"""
        try:
            # Create DatabaseConfig from successful connection
            config = DatabaseConfig(
                host=connection.info.host,
                port=connection.info.port,
                dbname=connection.info.dbname,
                user=connection.info.user,
                password=connection.info.password
            )
            
            # Initialize preprocessor with existing connection
            self.preprocessor = QueryPreprocessor(config)
            self.preprocessor.connection = connection
            
            # Initialize plan modifier
            self.modifier = QueryPlanModifier()
            
            # Setup event handlers
            self._setup_event_handlers()
            
            self.logger.info("System components initialized successfully after login")

        except Exception as e:
            self.logger.error(f"Error during post-login setup: {str(e)}")
            self.gui.show_error(f"Error during setup: {str(e)}")

    def _setup_event_handlers(self):
        """Connect GUI events to their handlers"""
        if not self.gui:
            return

        # Connect GUI events to corresponding methods
        self.gui.bind('<Generate>', self.handle_generate_plan)
        self.gui.bind('<OperatorChange>', self.handle_operator_modification)
        self.gui.bind('<JoinOrderChange>', self.handle_join_order_modification)
        self.gui.bind('<Reset>', self.handle_reset)

    def handle_generate_plan(self, sql: str) -> None:
        """Handle generation of initial query plan"""
        try:
            # Validate SQL
            is_valid, error = self.preprocessor.validate_sql(sql)
            if not is_valid:
                self.gui.show_error(f"Invalid SQL: {error}")
                return
            
            # Get initial query plan
            initial_plan = self.preprocessor.get_query_plan(sql)
            
            # Analyze plan complexity
            complexity_metrics = self.preprocessor.analyze_query_complexity(initial_plan)
            
            # Set up the plan modifier
            self.modifier.set_original_plan(initial_plan)
            
            # Update GUI with plan and metrics
            self.gui.update_plan_visualization(initial_plan)
            self.gui.update_metrics_display(complexity_metrics)
            
            self.logger.info("Query plan generated successfully")

        except Exception as e:
            self.logger.error(f"Error generating query plan: {str(e)}")
            self.gui.show_error(f"Error generating plan: {str(e)}")

    def run(self):
        """Run the application"""
        try:
            # Initialize components
            self.initialize()
            
            # Start the GUI main loop
            if self.gui:
                self.gui.mainloop()

        except Exception as e:
            self.logger.error(f"Error running application: {str(e)}")
            raise

        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        try:
            if self.preprocessor:
                self.preprocessor.disconnect()
            self.logger.info("Cleanup completed successfully")

        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

if __name__ == "__main__":
    try:
        # Create and run the application
        app = QueryPlanAnalysisSystem()
        app.run()

    except Exception as e:
        logging.error(f"Application failed to start: {str(e)}")
        sys.exit(1)