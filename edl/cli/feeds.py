import os

def list(energy_dashboard_path):
    return os.listdir(os.path.join(energy_dashboard_path, "data"))
