import pandas as pd
from app.core.mapping_engine import apply_mapping
from app.core.mapping_detector import detect_mapping

def transform_excel(input_path, output_path):
    df = pd.read_excel(input_path)

    mapping = detect_mapping(df.columns)

    final_df = apply_mapping(df, mapping)
    final_df.to_excel(output_path, index=False)