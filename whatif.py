from typing import Dict, Any, List, Optional, Tuple
import copy
import logging
from dataclasses import dataclass
from enum import Enum

class PlannerMethod(Enum):
    ENABLE_HASHJOIN = "enable_hashjoin"
    ENABLE_MERGEJOIN = "enable_mergejoin"
    ENABLE_NESTLOOP = "enable_nestloop"
    ENABLE_INDEXSCAN = "enable_indexscan"
    ENABLE_SEQSCAN = "enable_seqscan"
    ENABLE_BITMAPSCAN = "enable_bitmapscan"

@dataclass
class PlanModification:
    node_id: str
    current_type: str
    target_type: str
    affected_tables: List[str]

class QueryPlanModifier:
    def __init__(self):
        self._setup_logging()
        self.original_plan = None
        self.modified_plan = None
        self.modifications = []

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def set_original_plan(self, plan: Dict[str, Any]) -> None:
        """Store the original query plan"""
        self.original_plan = copy.deepcopy(plan)
        self.modified_plan = copy.deepcopy(plan)

    def generate_planner_hints(self, modifications: List[PlanModification]) -> Dict[str, bool]:
        """Generate PostgreSQL planner method settings based on desired modifications"""
        hints = {method.value: True for method in PlannerMethod}
        
        for mod in modifications:
            if mod.current_type != mod.target_type:
                # Disable current operation type
                if "Join" in mod.current_type:
                    hints[f"enable_{mod.current_type.lower().replace(' ', '')}"] = False
                elif "Scan" in mod.current_type:
                    hints[f"enable_{mod.current_type.lower().replace(' ', '')}"] = False
                
                # Enable target operation type
                if "Join" in mod.target_type:
                    hints[f"enable_{mod.target_type.lower().replace(' ', '')}"] = True
                elif "Scan" in mod.target_type:
                    hints[f"enable_{mod.target_type.lower().replace(' ', '')}"] = True
        
        return hints

    def modify_join_order(self, node_ids: List[str]) -> bool:
        """Modify the join order in the query plan"""
        if not self.modified_plan:
            return False

        def find_node(plan: Dict[str, Any], node_id: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
            """Find a node and its parent in the plan tree"""
            def traverse(node: Dict[str, Any], parent: Optional[Dict[str, Any]] = None):
                if id(node) == int(node_id):
                    return node, parent
                for child in node.get('Plans', []):
                    result = traverse(child, node)
                    if result[0]:
                        return result
                return None, None

            return traverse(plan['Plan'])

        try:
            # Collect all nodes
            nodes = []
            parents = []
            for node_id in node_ids:
                node, parent = find_node(self.modified_plan, node_id)
                if node:
                    nodes.append(node)
                    parents.append(parent)
                else:
                    return False

            # Perform the reordering
            for i in range(len(nodes) - 1):
                nodes[i]['Plans'], nodes[i + 1]['Plans'] = \
                    nodes[i + 1]['Plans'], nodes[i]['Plans']

            return True
        except Exception as e:
            self.logger.error(f"Error modifying join order: {str(e)}")
            return False

    def modify_operator(self, node_id: str, new_type: str) -> bool:
        """Modify an operator in the query plan"""
        if not self.modified_plan:
            return False

        def traverse_and_modify(node: Dict[str, Any]) -> bool:
            if id(node) == int(node_id):
                old_type = node['Node Type']
                node['Node Type'] = new_type
                self.modifications.append(
                    PlanModification(
                        node_id=node_id,
                        current_type=old_type,
                        target_type=new_type,
                        affected_tables=[node.get('Relation Name')] if 'Relation Name' in node else []
                    )
                )
                return True
                
            for child in node.get('Plans', []):
                if traverse_and_modify(child):
                    return True
            return False

        try:
            return traverse_and_modify(self.modified_plan['Plan'])
        except Exception as e:
            self.logger.error(f"Error modifying operator: {str(e)}")
            return False

    def generate_modified_sql(self, original_sql: str) -> str:
        """Generate modified SQL with planner hints"""
        hints = self.generate_planner_hints(self.modifications)
        hint_string = "/*+ "
        
        # Add join hints
        for mod in self.modifications:
            if "Join" in mod.target_type:
                hint_string += f"{mod.target_type.replace(' ', '')}({','.join(mod.affected_tables)}) "
        
        # Add scan hints
        for mod in self.modifications:
            if "Scan" in mod.target_type:
                hint_string += f"{mod.target_type.replace(' ', '')}({mod.affected_tables[0]}) "
        
        hint_string += "*/ "
        
        return hint_string + original_sql

    def reset_modifications(self) -> None:
        """Reset all modifications"""
        self.modified_plan = copy.deepcopy(self.original_plan)
        self.modifications = []

    def compare_plans(self) -> Dict[str, Any]:
        """Compare original and modified plans"""
        if not self.original_plan or not self.modified_plan:
            return {}

        def extract_metrics(plan: Dict[str, Any]) -> Dict[str, Any]:
            return {
                'total_cost': plan['Plan'].get('Total Cost', 0),
                'startup_cost': plan['Plan'].get('Startup Cost', 0),
                'plan_rows': plan['Plan'].get('Plan Rows', 0),
                'plan_width': plan['Plan'].get('Plan Width', 0)
            }

        original_metrics = extract_metrics(self.original_plan)
        modified_metrics = extract_metrics(self.modified_plan)

        comparison = {
            'cost_difference': modified_metrics['total_cost'] - original_metrics['total_cost'],
            'cost_percentage': (
                (modified_metrics['total_cost'] - original_metrics['total_cost']) 
                / original_metrics['total_cost'] * 100
            ),
            'original_metrics': original_metrics,
            'modified_metrics': modified_metrics
        }

        return comparison