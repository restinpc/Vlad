import requests
import json
import os
import time
import concurrent.futures
import mysql.connector
from datetime import datetime
from urllib.parse import urljoin
from dotenv import load_dotenv

# ========== КОНФИГУРАЦИЯ ==========
load_dotenv()

BASE_API_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
OUTPUT_DIR = "treasury_datasets_json"
PAGE_SIZE = 10000
MAX_WORKERS = 3
MAX_RETRIES = 10

# (Словарь)
DATASETS = {
    "Daily_Treasury_Statement_All": "v1/accounting/dts/dts_all",
    "Judgment_Fund_Report": "v2/payments/jfics/jfics_congress_report",
    "MSPD_Table_1": "v1/debt/mspd/mspd_table_1",
    "MSPD_Table_2": "v1/debt/mspd/mspd_table_2",
    "MSPD_Table_3": "v1/debt/mspd/mspd_table_3",
    "MSPD_Table_3_Market": "v1/debt/mspd/mspd_table_3_market",
    "MSPD_Table_3_NonMarket": "v1/debt/mspd/mspd_table_3_nonmarket",
    "MSPD_Table_4": "v1/debt/mspd/mspd_table_4",
    "MSPD_Table_5": "v1/debt/mspd/mspd_table_5",
    "MTS_Table_1": "v1/accounting/mts/mts_table_1",
    "MTS_Table_2": "v1/accounting/mts/mts_table_2",
    "MTS_Table_3": "v1/accounting/mts/mts_table_3",
    "MTS_Table_4": "v1/accounting/mts/mts_table_4",
    "MTS_Table_5": "v1/accounting/mts/mts_table_5",
    "MTS_Table_5m": "v1/accounting/mts/mts_table_5m",
    "MTS_Table_6": "v1/accounting/mts/mts_table_6",
    "MTS_Table_6a": "v1/accounting/mts/mts_table_6a",
    "MTS_Table_6b": "v1/accounting/mts/mts_table_6b",
    "MTS_Table_6c": "v1/accounting/mts/mts_table_6c",
    "MTS_Table_6d": "v1/accounting/mts/mts_table_6d",
    "MTS_Table_6e": "v1/accounting/mts/mts_table_6e",
    "MTS_Table_7": "v1/accounting/mts/mts_table_7",
    "MTS_Table_8": "v1/accounting/mts/mts_table_8",
    "MTS_Table_9": "v1/accounting/mts/mts_table_9",
    "Ag_Disaster_Relief_Expected": "v1/accounting/od/agriculture_disaster_relief_trust_fund_expected",
    "Ag_Disaster_Relief_Results": "v1/accounting/od/agriculture_disaster_relief_trust_fund_results",
    "Airport_Airway_Trust_Expected": "v1/accounting/od/airport_airway_trust_fund_expected",
    "Airport_Airway_Trust_Results": "v1/accounting/od/airport_airway_trust_fund_results",
    "Auctions_Query": "v1/accounting/od/auctions_query",
    "Average_Interest_Rates": "v2/accounting/od/avg_interest_rates",
    "Balance_Sheets": "v2/accounting/od/balance_sheets",
    "Black_Lung_Trust_Expected": "v1/accounting/od/black_lung_disability_trust_fund_expected",
    "Black_Lung_Trust_Results": "v1/accounting/od/black_lung_disability_trust_fund_results",
    "Buybacks_Operations": "v1/accounting/od/buybacks_operations",
    "Buybacks_Security_Details": "v1/accounting/od/buybacks_security_details",
    "FBP_Balances": "v1/accounting/od/fbp_balances",
    "FBP_Future_Transactions": "v1/accounting/od/fbp_future_dated_transactions",
    "FBP_GL_Borrowing_Balances": "v1/accounting/od/fbp_gl_borrowing_balances",
    "FBP_GL_Repay_Advance_Balances": "v1/accounting/od/fbp_gl_repay_advance_balances",
    "Federal_Maturity_Rates": "v1/accounting/od/federal_maturity_rates",
    "FIP_Principal_Outstanding_T1": "v1/accounting/od/fip_principal_outstanding_table1",
    "FIP_Principal_Outstanding_T2": "v1/accounting/od/fip_principal_outstanding_table2",
    "FIP_Statement_Account_T1": "v1/accounting/od/fip_statement_of_account_table1",
    "FIP_Statement_Account_T2": "v1/accounting/od/fip_statement_of_account_table2",
    "FIP_Statement_Account_T3": "v1/accounting/od/fip_statement_of_account_table3",
    "FRN_Daily_Indexes": "v1/accounting/od/frn_daily_indexes",
    "GAS_Daily_Activity_Totals": "v1/accounting/od/gas_daily_activity_totals",
    "GAS_Held_By_Public": "v1/accounting/od/gas_held_by_public_daily_activity",
    "GAS_Intragov_Holdings": "v1/accounting/od/gas_intragov_holdings_daily_activity",
    "Gift_Contributions": "v2/accounting/od/gift_contributions",
    "Gold_Reserve": "v2/accounting/od/gold_reserve",
    "Harbor_Maintenance_Expected": "v1/accounting/od/harbor_maintenance_trust_fund_expected",
    "Harbor_Maintenance_Results": "v1/accounting/od/harbor_maintenance_trust_fund_results",
    "Hazardous_Substance_Expected": "v1/accounting/od/hazardous_substance_superfund_expected",
    "Hazardous_Substance_Results": "v1/accounting/od/hazardous_substance_superfund_results",
    "Highway_Trust_Fund": "v1/accounting/od/highway_trust_fund",
    "Highway_Trust_Fund_Expected": "v1/accounting/od/highway_trust_fund_expected",
    "Highway_Trust_Fund_Results": "v1/accounting/od/highway_trust_fund_results",
    "Inland_Waterways_Expected": "v1/accounting/od/inland_waterways_trust_fund_expected",
    "Inland_Waterways_Results": "v1/accounting/od/inland_waterways_trust_fund_results",
    "Interest_Cost_Fund": "v2/accounting/od/interest_cost_fund",
    "Interest_Expense": "v2/accounting/od/interest_expense",
    "Interest_Uninvested": "v2/accounting/od/interest_uninvested",
    "Leaking_Underground_Tank_Expected": "v1/accounting/od/leaking_underground_storage_tank_trust_fund_expected",
    "Leaking_Underground_Tank_Results": "v1/accounting/od/leaking_underground_storage_tank_trust_fund_results",
    "Nuclear_Waste_Fund_Results": "v1/accounting/od/nuclear_waste_fund_results",
    "Oil_Spill_Liability_Expected": "v1/accounting/od/oil_spill_liability_trust_fund_expected",
    "Oil_Spill_Liability_Results": "v1/accounting/od/oil_spill_liability_trust_fund_results",
    "Debt_Outstanding": "v2/accounting/od/debt_outstanding",
    "Patient_Centered_Research_Expected": "v1/accounting/od/patient_centered_outcomes_research_trust_fund_expected",
    "Patient_Centered_Research_Results": "v1/accounting/od/patient_centered_outcomes_research_trust_fund_results",
    "Qualified_Tax": "v2/accounting/od/qualified_tax",
    "Rates_of_Exchange": "v1/accounting/od/rates_of_exchange",
    "Receipts_by_Department": "v1/accounting/od/receipts_by_department",
    "Record_Setting_Auction": "v2/accounting/od/record_setting_auction",
    "Redemption_Tables": "v2/accounting/od/redemption_tables",
    "Reforestation_Trust_Expected": "v1/accounting/od/reforestation_trust_fund_expected",
    "Reforestation_Trust_Results": "v1/accounting/od/reforestation_trust_fund_results",
    "Savings_Bonds_MUD": "v1/accounting/od/savings_bonds_mud",
    "Savings_Bonds_PCS": "v1/accounting/od/savings_bonds_pcs",
    "Savings_Bonds_Report": "v1/accounting/od/savings_bonds_report",
    "Savings_Bonds_Value": "v2/accounting/od/sb_value",
    "Schedules_Fed_Daily_Activity": "v1/accounting/od/schedules_fed_debt_daily_activity",
    "Schedules_Fed_Daily_Summary": "v1/accounting/od/schedules_fed_debt_daily_summary",
    "Schedules_Fed_Debt": "v1/accounting/od/schedules_fed_debt",
    "Schedules_Fed_FYTD": "v1/accounting/od/schedules_fed_debt_fytd",
    "Securities_Accounts": "v1/accounting/od/securities_accounts",
    "Securities_C_of_I": "v1/accounting/od/securities_c_of_i",
    "Securities_Redemptions": "v1/accounting/od/securities_redemptions",
    "Securities_Sales": "v1/accounting/od/securities_sales",
    "SLGS_Demand_Deposit_Rates": "v1/accounting/od/slgs_demand_deposit_rates",
    "SLGS_Savings_Bonds": "v1/accounting/od/slgs_savings_bonds",
    "SLGS_Securities": "v1/accounting/od/slgs_securities",
    "SLGS_Statistics": "v2/accounting/od/slgs_statistics",
    "SLGS_Time_Deposit_Rates": "v1/accounting/od/slgs_time_deposit_rates",
    "Sport_Fish_Restoration_Expected": "v1/accounting/od/sport_fish_restoration_boating_trust_fund_expected",
    "Sport_Fish_Restoration_Results": "v1/accounting/od/sport_fish_restoration_boating_trust_fund_results",
    "Statement_Net_Cost": "v2/accounting/od/statement_net_cost",
    "TCIR_Annual_Table_1": "v1/accounting/od/tcir_annual_table_1",
    "TCIR_Annual_Table_2": "v1/accounting/od/tcir_annual_table_2",
    "TCIR_Annual_Table_3": "v1/accounting/od/tcir_annual_table_3",
    "TCIR_Annual_Table_4": "v1/accounting/od/tcir_annual_table_4",
    "TCIR_Annual_Table_5": "v1/accounting/od/tcir_annual_table_5",
    "TCIR_Annual_Table_6": "v1/accounting/od/tcir_annual_table_6",
    "TCIR_Annual_Table_7": "v1/accounting/od/tcir_annual_table_7",
    "TCIR_Annual_Table_8": "v1/accounting/od/tcir_annual_table_8",
    "TCIR_Annual_Table_9": "v1/accounting/od/tcir_annual_table_9",
    "TCIR_Monthly_Table_1": "v1/accounting/od/tcir_monthly_table_1",
    "TCIR_Monthly_Table_2": "v1/accounting/od/tcir_monthly_table_2",
    "TCIR_Monthly_Table_3": "v1/accounting/od/tcir_monthly_table_3",
    "TCIR_Monthly_Table_4": "v1/accounting/od/tcir_monthly_table_4",
    "TCIR_Monthly_Table_5": "v1/accounting/od/tcir_monthly_table_5",
    "TCIR_Monthly_Table_6": "v1/accounting/od/tcir_monthly_table_6",
    "TCIR_Quarterly_Table_2a": "v1/accounting/od/tcir_quarterly_table_2a",
    "TCIR_Quarterly_Table_2b": "v1/accounting/od/tcir_quarterly_table_2b",
    "TCIR_Quarterly_Table_3": "v1/accounting/od/tcir_quarterly_table_3",
    "TCIR_Semi_Annual": "v1/accounting/od/tcir_semi_annual",
    "TIPS_CPI_Data": "v1/accounting/od/tips_cpi_data",
    "TMA_Contract_Disputes": "v1/accounting/od/tma_contract_disputes",
    "TMA_No_Fear": "v1/accounting/od/tma_no_fear",
    "TMA_Unclaimed_Money": "v1/accounting/od/tma_unclaimed_money",
    "Debt_to_the_Penny": "v2/accounting/od/debt_to_penny",
    "Upcoming_Auctions": "v1/accounting/od/upcoming_auctions",
    "Uranium_Enrichment_Expected": "v1/accounting/od/uranium_enrichment_decontamination_decommissioning_fund_expected",
    "Uranium_Enrichment_Results": "v1/accounting/od/uranium_enrichment_decontamination_decommissioning_fund_results",
    "Victims_Terrorism_Fund_Expected": "v1/accounting/od/us_victims_state_sponsored_terrorism_fund_expected",
    "Victims_Terrorism_Fund_Results": "v1/accounting/od/us_victims_state_sponsored_terrorism_fund_results",
    "UTF_Account_Statement": "v1/accounting/od/utf_account_statement",
    "UTF_Federal_Activity": "v1/accounting/od/utf_federal_activity_statement",
    "UTF_Quarterly_Yields": "v2/accounting/od/utf_qtr_yields",
    "UTF_Transaction_Statement": "v1/accounting/od/utf_transaction_statement",
    "Vaccine_Injury_Expected": "v1/accounting/od/vaccine_injury_compensation_trust_fund_expected",
    "Vaccine_Injury_Results": "v1/accounting/od/vaccine_injury_compensation_trust_fund_results",
    "Wool_Research_Expected": "v1/accounting/od/wool_research_development_promotion_trust_fund_expected",
    "Wool_Research_Results": "v1/accounting/od/wool_research_development_promotion_trust_fund_results",
    "Revenue_Collections_RCM": "v2/revenue/rcm",
    "FMR_Data_Elements": "v1/reference/data_registry/fmr_data_elements",
    "FMR_Enum_Values": "v1/reference/data_registry/fmr_enum_values",
    "FMR_Situational_Metadata": "v1/reference/data_registry/fmr_situational_metadata",
    "TOP_Federal": "v1/debt/top/top_federal",
    "TOP_State": "v1/debt/top/top_state",
    "Treasury_Offset_Program": "v1/debt/treasury_offset_program",
    "TROR_Main": "v2/debt/tror",
    "TROR_Collected_Outstanding": "v2/debt/tror/collected_outstanding_recv",
    "TROR_Collections_Delinquent": "v2/debt/tror/collections_delinquent_debt",
    "TROR_Data_Act_Compliance": "v2/debt/tror/data_act_compliance",
    "TROR_Delinquent_Debt": "v2/debt/tror/delinquent_debt",
    "TROR_Written_Off": "v2/debt/tror/written_off_delinquent_debt",
}


# ========== БЛОК СКАЧИВАНИЯ ==========
def fetch_page_robust(full_url, page_num):
    params = {'page[number]': page_num, 'page[size]': PAGE_SIZE, 'format': 'json'}
    attempts = 0
    while attempts < MAX_RETRIES:
        try:
            with requests.Session() as s:
                resp = s.get(full_url, params=params, timeout=45)
                if resp.status_code == 429:
                    time.sleep((attempts + 2) * 2)
                    attempts += 1
                    continue
                resp.raise_for_status()
                return resp.json().get('data', [])
        except Exception:
            time.sleep((attempts + 1) * 3)
            attempts += 1
    return None

def download_dataset_robust(name, endpoint):
    full_url = urljoin(BASE_API_URL, endpoint)
    filename = f"{name}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)
    if os.path.exists(filepath):
        print(f"✓ {name}: готов")
        return True
    print(f"🚀 Скачивание: {name}...")
    try:
        with requests.Session() as s:
            resp = s.get(full_url, params={'page[number]': 1, 'page[size]': PAGE_SIZE, 'format': 'json'}, timeout=45)
            if resp.status_code == 404: return False
            resp.raise_for_status()
            data_json = resp.json()
    except Exception:
        return False

    meta = data_json.get('meta', {})
    total_pages = meta.get('total-pages', 1)
    all_data = data_json.get('data', [])
    if total_pages > 1:
        pages_to_fetch = range(2, total_pages + 1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_page = {executor.submit(fetch_page_robust, full_url, p): p for p in pages_to_fetch}
            for future in concurrent.futures.as_completed(future_to_page):
                page_data = future.result()
                if page_data: all_data.extend(page_data)
    save_json(filepath, all_data, meta)
    return True

def save_json(filepath, data, metadata):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump({'metadata': metadata, 'data': data}, f, ensure_ascii=False, indent=2)

# ========== БЛОК ЗАГРУЗКИ В ОТДЕЛЬНЫЕ ТАБЛИЦЫ ==========

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"), user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"), database=os.getenv("DB_NAME")
    )

def create_table_dynamic(cursor, table_name, sample_data):
    """Создает таблицу с колонками, соответствующими полям JSON."""
    columns_def = []
    for key in sample_data.keys():
        # Очищаем имя колонки от пробелов и дефисов
        safe_col = key.replace("-", "_").replace(" ", "_").replace(".", "").lower()
        columns_def.append(f"`{safe_col}` TEXT")

    # Собираем SQL
    cols_sql = ",\n".join(columns_def)
    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {cols_sql},
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        cursor.execute(sql)
    except mysql.connector.Error as err:
        print(f"⚠️ Ошибка создания {table_name}: {err}")

def load_file_to_separate_table(filename):
    filepath = os.path.join(OUTPUT_DIR, filename)
    dataset_name = filename.replace(".json", "").lower()

    # Формируем имя таблицы: vlad_treasury_ + имя_файла
    table_name = f"vlad_treasury_{dataset_name}"[:64]

    print(f"📥 {dataset_name} -> таблица `{table_name}`")

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = json.load(f)
            data = content.get('data', [])
    except Exception:
        return

    if not data:
        print("   (пусто)")
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # 1. Создаем таблицу по образу первой строки
        create_table_dynamic(cursor, table_name, data[0])

        # 2. Готовим SQL вставки
        keys = list(data[0].keys())
        safe_keys = [k.replace("-", "_").replace(" ", "_").replace(".", "").lower() for k in keys]

        placeholders = ", ".join(["%s"] * len(keys))
        columns_str = ", ".join([f"`{k}`" for k in safe_keys])

        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        # 3. Вставляем пачками
        batch_size = 1000
        batch = []
        total = 0

        for row in data:
            values = []
            for k in keys:
                val = row.get(k)
                if isinstance(val, (dict, list)): val = json.dumps(val)  # Если внутри вложенный JSON
                if val == "": val = None
                values.append(val)

            batch.append(tuple(values))

            if len(batch) >= batch_size:
                cursor.executemany(insert_query, batch)
                conn.commit()
                total += len(batch)
                batch = []
                print(f"   ... {total}", end='\r')

        if batch:
            cursor.executemany(insert_query, batch)
            conn.commit()
            total += len(batch)

        print(f"   ✅ Загружено {total} строк")

    except mysql.connector.Error as err:
        print(f"   ❌ Ошибка SQL: {err}")
    finally:
        cursor.close()
        conn.close()


def main():
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)

    # 1. СКАЧИВАНИЕ (вставьте полный DATASETS выше!)
    print("\n=== ЭТАП 1: ПРОВЕРКА ФАЙЛОВ ===")
    for name, endpoint in DATASETS.items():
        download_dataset_robust(name, endpoint)

    # 2. ЗАГРУЗКА
    print("\n=== ЭТАП 2: ЗАГРУЗКА В ОТДЕЛЬНЫЕ ТАБЛИЦЫ ===")
    files = [f for f in os.listdir(OUTPUT_DIR) if f.endswith(".json")]

    for filename in files:
        load_file_to_separate_table(filename)

    print("\n🏁 ГОТОВО")


if __name__ == "__main__":
    main()
