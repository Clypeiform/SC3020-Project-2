import sys
from typing import Optional
import logging
from interface import QueryPlanAnalyzer
from preprocessing import QueryPreprocessor, DatabaseConfig
from whatif import QueryPlanModifier

class QueryPlanAnalysisSystem:
    def __init__(self):
        self._setup_logging()
        self.config = self._load_config()
        self.preprocessor = QueryPreprocessor(self.config)
        self.modifier = QueryPlanModifier()
        self.gui: Optional[QueryPlanAnalyzer] = None

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

    def _load_config(self) -> DatabaseConfig:
        config = DatabaseConfig()
        is_valid, error = DatabaseConfig.validate_config()
        
        if not is_valid:
            self.logger.error(f"Invalid database configuration: {error}")
            raise ValueError(f"Invalid database configuration: {error}")
            
        self.logger.info("Database configuration loaded successfully")
        return config

    def initialize(self):
        """Initialize the system components"""
        try:
            # Initialize database connection
            self.preprocessor.connect()
            
            # Create and configure GUI
            self.gui = QueryPlanAnalyzer()
            
            # Connect GUI events to handlers
            self._setup_event_handlers()
            
            self.logger.info("System initialized successfully")
        except Exception as e:
            self.logger.error(f"Error during system initialization: {str(e)}")
            raise
    def _setup_event_handlers(self):
            """Connect GUI events to their handlers"""
            if not self.gui:
                return

            # Connect GUI events to corresponding methods
            self.gui.bind_generate_plan(self.handle_generate_plan)
            self.gui.bind_operator_change(self.handle_operator_modification)
            self.gui.bind_join_order_change(self.handle_join_order_modification)
            self.gui.bind_reset(self.handle_reset)

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

    def handle_operator_modification(self, node_id: str, new_operator_type: str) -> None:
        """Handle modification of an operator in the query plan"""
        try:
            # Attempt to modify the operator
            success = self.modifier.modify_operator(node_id, new_operator_type)
            
            if not success:
                self.gui.show_error("Failed to modify operator")
                return
                
            # Generate modified SQL with hints
            modified_sql = self.modifier.generate_modified_sql(
                self.gui.get_current_sql()
            )
            
            # Get new execution plan
            new_plan = self.preprocessor.get_query_plan(modified_sql)
            
            # Compare plans
            comparison = self.modifier.compare_plans()
            
            # Update GUI with new plan and comparison
            self.gui.update_alternative_plan(new_plan)
            self.gui.update_cost_comparison(comparison)
            
            self.logger.info(f"Operator modified successfully: {node_id} -> {new_operator_type}")
            
        except Exception as e:
            self.logger.error(f"Error modifying operator: {str(e)}")
            self.gui.show_error(f"Error modifying operator: {str(e)}")

    def handle_join_order_modification(self, node_ids: list) -> None:
        """Handle modification of join order in the query plan"""
        try:
            # Attempt to modify join order
            success = self.modifier.modify_join_order(node_ids)
            
            if not success:
                self.gui.show_error("Failed to modify join order")
                return
                
            # Generate modified SQL with hints
            modified_sql = self.modifier.generate_modified_sql(
                self.gui.get_current_sql()
            )
            
            # Get new execution plan
            new_plan = self.preprocessor.get_query_plan(modified_sql)
            
            # Compare plans
            comparison = self.modifier.compare_plans()
            
            # Update GUI with new plan and comparison
            self.gui.update_alternative_plan(new_plan)
            self.gui.update_cost_comparison(comparison)
            
            self.logger.info("Join order modified successfully")
            
        except Exception as e:
            self.logger.error(f"Error modifying join order: {str(e)}")
            self.gui.show_error(f"Error modifying join order: {str(e)}")

    def handle_reset(self) -> None:
        """Handle reset of modifications"""
        try:
            self.modifier.reset_modifications()
            self.gui.reset_alternative_plan()
            self.gui.reset_cost_comparison()
            
            self.logger.info("Plan modifications reset successfully")
            
        except Exception as e:
            self.logger.error(f"Error resetting modifications: {str(e)}")
            self.gui.show_error(f"Error resetting modifications: {str(e)}")

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
            # Clean up
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        try:
            # Close database connection
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