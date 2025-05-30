"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    import pandas as pd # type: ignore
    from zipfile import ZipFile
    import glob
    import os

    # Create output directory
    os.makedirs('files/output', exist_ok=True)

    # Read all zip files from input directory
    dfs = []
    for zip_file in glob.glob('files/input/*.zip'):
        with ZipFile(zip_file) as zf:
            for filename in zf.namelist():
                if filename.endswith('.csv'):
                    with zf.open(filename) as f:
                        df = pd.read_csv(f)
                        dfs.append(df)
    
    # Combine all dataframes
    data = pd.concat(dfs, ignore_index=True)

    # Process client data
    client_df = data[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()
    client_df['job'] = client_df['job'].str.replace('.', '').str.replace('-', '_')
    client_df['education'] = client_df['education'].str.replace('.', '_')
    client_df['education'] = client_df['education'].replace('unknown', pd.NA)
    client_df['credit_default'] = (client_df['credit_default'] == 'yes').astype(int)
    client_df['mortgage'] = (client_df['mortgage'] == 'yes').astype(int)

    # Process campaign data
    campaign_df = data[['client_id', 'number_contacts', 'contact_duration','previous_campaign_contacts', 'previous_outcome', 
                       'campaign_outcome', 'day', 'month']].copy()
    campaign_df['previous_outcome'] = (campaign_df['previous_outcome'] == 'success').astype(int)
    campaign_df['campaign_outcome'] = (campaign_df['campaign_outcome'] == 'yes').astype(int)
    
    # Create date column combining day, month with year 2022
    campaign_df['last_contact_date'] = pd.to_datetime(
        '2022-' + campaign_df['month'].astype(str) + '-' + campaign_df['day'].astype(str),
        format='%Y-%b-%d'
    ).dt.strftime('%Y-%m-%d')
    
    # Drop original day and month columns
    campaign_df = campaign_df.drop(['day', 'month'], axis=1)

    # Process economics data
    economics_df = data[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()

    # Save processed dataframes to CSV files
    client_df.to_csv('files/output/client.csv', index=False)
    campaign_df.to_csv('files/output/campaign.csv', index=False)
    economics_df.to_csv('files/output/economics.csv', index=False)


if __name__ == "__main__":
    clean_campaign_data()
