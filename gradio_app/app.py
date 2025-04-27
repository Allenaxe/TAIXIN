import gradio as gr
import requests
import json

API_URL = "https://52mlgsjekh.execute-api.us-west-2.amazonaws.com/prod/predict"

# 呼叫API的function
def call_api(acct_nbr, cust_id, tx_date, tx_time, drcr, tx_amt, pb_bal,
             own_trans_acct, own_trans_id, channel_code, trn_code, branch_no,
             emp_no, mb_check, eb_check, same_number_ip, same_number_uuid,
             day_of_week, cancel_no_contact, is_digital, acct_open_dt, aum_amt,
             date_of_birth, yearly_income_level, cnty_cd):

    payload = {
        "ACCT_NBR": acct_nbr,
        "CUST_ID": cust_id,
        "TX_DATE": tx_date,
        "TX_TIME": tx_time,
        "DRCR": drcr,
        "TX_AMT": tx_amt,
        "PB_BAL": pb_bal,
        "OWN_TRANS_ACCT": own_trans_acct,
        "OWN_TRANS_ID": own_trans_id,
        "CHANNEL_CODE": channel_code,
        "TRN_CODE": trn_code,
        "BRANCH_NO": branch_no,
        "EMP_NO": emp_no,
        "mb_check": mb_check,
        "eb_check": eb_check,
        "SAME_NUMBER_IP": same_number_ip,
        "SAME_NUMBER_UUID": same_number_uuid,
        "DAY_OF_WEEK": day_of_week,
        "CANCEL_NO_CONTACT": cancel_no_contact,
        "IS_DIGITAL": is_digital,
        "ACCT_OPEN_DT": acct_open_dt,
        "AUM_AMT": aum_amt,
        "DATE_OF_BIRTH": date_of_birth,
        "YEARLYINCOMELEVEL": yearly_income_level,
        "CNTY_CD": cnty_cd
    }

    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(API_URL, headers=headers, json=payload)
        if response.status_code == 200:
            result = response.json()
            return f"🌟 風險標籤: {result['risk_label']}\n\n🌟 信心水準: {result['risk_score']:.2f}\n\n🌟 解釋:\n{result['explanation']}"
        else:
            return f"❌ 錯誤！Status: {response.status_code}\n內容: {response.text}"
    except Exception as e:
        return f"❌ 發生錯誤: {str(e)}"

# 定義 Gradio inputs
inputs = [
    gr.Textbox(label="ACCT_NBR (帳號)"),
    gr.Textbox(label="CUST_ID (客戶ID)"),
    gr.Textbox(label="TX_DATE (交易日期)"),
    gr.Textbox(label="TX_TIME (交易時間)"),
    gr.Textbox(label="DRCR (借記D/貸記C)"),
    gr.Number(label="TX_AMT (交易金額)"),
    gr.Number(label="PB_BAL (戶頭餘額)"),
    gr.Textbox(label="OWN_TRANS_ACCT (交易對手帳號)"),
    gr.Textbox(label="OWN_TRANS_ID (交易對手ID)"),
    gr.Textbox(label="CHANNEL_CODE (交易渠道代碼)"),
    gr.Textbox(label="TRN_CODE (交易類型代碼)"),
    gr.Textbox(label="BRANCH_NO (分行代號)"),
    gr.Textbox(label="EMP_NO (行員代號)"),
    gr.Textbox(label="mb_check (行動銀行查詢次數)"),
    gr.Textbox(label="eb_check (網路銀行查詢次數)"),
    gr.Textbox(label="SAME_NUMBER_IP (是否IP共號 0/1)"),
    gr.Textbox(label="SAME_NUMBER_UUID (是否UUID共號 0/1)"),
    gr.Number(label="DAY_OF_WEEK (星期幾 0-6)"),
    gr.Textbox(label="CANCEL_NO_CONTACT (解除久未往來 0/1)"),
    gr.Textbox(label="IS_DIGITAL (是否數位帳號 0/1)"),
    gr.Textbox(label="ACCT_OPEN_DT (開戶日)"),
    gr.Number(label="AUM_AMT (資產總額)"),
    gr.Textbox(label="DATE_OF_BIRTH (出生年月日)"),
    gr.Textbox(label="YEARLYINCOMELEVEL (年收入等級)"),
    gr.Textbox(label="CNTY_CD (國家代碼)")
]

examples = [
    # 更新後的新範例（應該是正常帳戶）
    [
        "ACCT6068", "ID5684", "18264", "7", "2",
        8531, 231802, "ACCT31429", "ID99999", "17", "25",
        "B111", "E4445", "0", "0", "0", "0",
        5, "0", "0", "8400", 256930,
        "61", "25", "12"
    ],
    # 第二個範例（原本你剛給的偏低風險）
    [
        "ACCT16300", "ID15460", "18285", "8", "2",
        1438, 4167, "ACCT31429", "ID99999", "17", "25",
        "B111", "E4603", "1", "0", "0", "1",
        5, "0", "1", "16254", 24430,
        "36", "25", "12"
    ]
]

# 建立Gradio介面
iface = gr.Interface(
    fn=call_api,
    inputs=inputs,
    outputs="text",
    title="台新交易帳戶風險預測小工具",
    description="輸入帳戶基本資料，或者直接點選下方範例，一鍵測試API推論結果！",
    examples=examples
)

if __name__ == "__main__":
    iface.launch(server_name="0.0.0.0", server_port=8080)