import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import json
from typing import Dict, Any, Optional
import psycopg2
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import networkx as nx

class LoginWindow(tk.Toplevel):
    def __init__(self, parent, callback):
        super().__init__(parent)
        self.callback = callback
        self.title("Database Login")
        self.geometry("300x250")
        
        # Remove centering relative to parent since parent is withdrawn
        self.geometry("+%d+%d" % (self.winfo_screenwidth()//4, self.winfo_screenheight()//4))
        
        # Create form
        ttk.Label(self, text="Database Connection", font=('Helvetica', 12, 'bold')).pack(pady=10)
        
        frame = ttk.Frame(self)
        frame.pack(padx=20, pady=10)
        
        # Database connection fields
        fields = [('Host:', 'localhost'), ('Port:', '5432'), 
                 ('Database:', 'TPC-H'), ('Username:', 'postgres'), 
                 ('Password:', '')]
        
        self.entries = {}
        for label, default in fields:
            row = ttk.Frame(frame)
            row.pack(fill='x', pady=5)
            ttk.Label(row, text=label, width=10).pack(side='left')
            entry = ttk.Entry(row)
            entry.insert(0, default)
            if 'Password' in label:
                entry.config(show='*')
            entry.pack(side='left', fill='x', expand=True)
            self.entries[label.replace(':', '').lower()] = entry
        
        # Connect button
        ttk.Button(self, text="Connect", command=self.connect).pack(pady=10)
        
        # Make modal but don't use transient since parent is withdrawn
        self.grab_set()
    
    def connect(self):
        try:
            conn = psycopg2.connect(
                host=self.entries['host'].get(),
                port=self.entries['port'].get(),
                dbname=self.entries['database'].get(),
                user=self.entries['username'].get(),
                password=self.entries['password'].get()
            )
            self.callback(conn)
            self.destroy()
        except Exception as e:
            messagebox.showerror("Connection Error", str(e))

class QueryPlanAnalyzer(tk.Tk):
    def __init__(self):
        super().__init__()
        
        # Initialize variables before showing login window
        self.title("Query Plan Analysis Tool")
        self.geometry("1200x800")
        self.qep_data = None
        self.aqp_data = None
        self.connection = None
        
        # Center the main window
        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        x = (screen_width - 1200) // 2
        y = (screen_height - 800) // 2
        self.geometry(f"1200x800+{x}+{y}")
        
        # Show login window
        self.withdraw()
        self.login_window = LoginWindow(self, self.on_login_success)
        
    def on_login_success(self, connection):
        self.connection = connection
        self.create_widgets()  # Create widgets before showing the window
        self.deiconify()      # Show the main window
        
    def create_widgets(self):
        # Create main container
        main_container = ttk.PanedWindow(self, orient=tk.HORIZONTAL)
        main_container.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Left panel for query input
        left_panel = ttk.Frame(main_container)
        main_container.add(left_panel)
        
        # Query input
        query_label = ttk.Label(left_panel, text="SQL Query:")
        query_label.pack(pady=5)
        
        self.query_text = scrolledtext.ScrolledText(left_panel, height=10)
        self.query_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Generate button
        self.generate_btn = ttk.Button(left_panel, text="Generate Query Plan", 
                                     command=self.generate_query_plan)
        self.generate_btn.pack(pady=10)
        
        # Right panel with notebook for QEP and AQP
        right_panel = ttk.Notebook(main_container)
        main_container.add(right_panel)
        
        # QEP tab
        self.qep_frame = ttk.Frame(right_panel)
        right_panel.add(self.qep_frame, text="Original QEP")
        
        # AQP tab
        self.aqp_frame = ttk.Frame(right_panel)
        right_panel.add(self.aqp_frame, text="Modified AQP")
        
        # Cost comparison panel at bottom
        cost_frame = ttk.LabelFrame(self, text="Cost Comparison")
        cost_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Cost labels
        self.qep_cost_label = ttk.Label(cost_frame, text="Original QEP Cost: N/A")
        self.qep_cost_label.pack(side=tk.LEFT, padx=20, pady=5)
        
        self.aqp_cost_label = ttk.Label(cost_frame, text="Modified AQP Cost: N/A")
        self.aqp_cost_label.pack(side=tk.LEFT, padx=20, pady=5)
        
        # Initialize visualization
        self.setup_tree_visualization()

    def setup_tree_visualization(self):
        """Setup the matplotlib figure for tree visualization"""
        self.qep_figure = Figure(figsize=(6, 4))
        self.qep_canvas = FigureCanvasTkAgg(self.qep_figure, self.qep_frame)
        self.qep_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        self.aqp_figure = Figure(figsize=(6, 4))
        self.aqp_canvas = FigureCanvasTkAgg(self.aqp_figure, self.aqp_frame)
        self.aqp_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    def generate_query_plan(self):
        """Generate query plan from input SQL"""
        sql = self.query_text.get("1.0", tk.END).strip()
        if not sql:
            return
            
        try:
            # Get query plan from PostgreSQL
            with self.connection.cursor() as cur:
                cur.execute(f"EXPLAIN (FORMAT JSON) {sql}")
                self.qep_data = cur.fetchone()[0]
            self.visualize_plan(self.qep_data, is_qep=True)
            self.update_cost_labels()
        except Exception as e:
            messagebox.showerror("Error", f"Error generating query plan: {str(e)}")

    def visualize_plan(self, plan_data: Dict[str, Any], is_qep: bool = True):
        """Visualize query plan as a tree using networkx and matplotlib"""
        G = nx.DiGraph()
        figure = self.qep_figure if is_qep else self.aqp_figure
        canvas = self.qep_canvas if is_qep else self.aqp_canvas
        
        # Clear previous visualization
        figure.clear()
        
        # Create graph from plan data
        self.build_plan_graph(G, plan_data[0]['Plan'], None)
        
        # Draw the graph
        ax = figure.add_subplot(111)
        pos = nx.spring_layout(G)
        nx.draw(G, pos, ax=ax, with_labels=True, node_color='lightblue',
                node_size=1500, font_size=8, font_weight='bold')
        
        canvas.draw()

    def build_plan_graph(self, G: nx.DiGraph, node: Dict[str, Any], parent_id: str):
        """Recursively build networkx graph from plan data"""
        node_id = f"{node['Node Type']}_{id(node)}"
        G.add_node(node_id, label=node['Node Type'])
        
        if parent_id:
            G.add_edge(parent_id, node_id)
            
        if 'Plans' in node:
            for child in node['Plans']:
                self.build_plan_graph(G, child, node_id)

    def update_cost_labels(self):
        """Update cost comparison labels"""
        if self.qep_data:
            qep_cost = self.qep_data[0]['Plan'].get('Total Cost', 'N/A')
            self.qep_cost_label.config(text=f"Original QEP Cost: {qep_cost}")
            
        if self.aqp_data:
            aqp_cost = self.aqp_data[0]['Plan'].get('Total Cost', 'N/A')
            self.aqp_cost_label.config(text=f"Modified AQP Cost: {aqp_cost}")

    def __del__(self):
        if self.connection:
            self.connection.close()

if __name__ == "__main__":
    app = QueryPlanAnalyzer()
    app.mainloop()