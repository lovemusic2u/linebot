import gspread, mysql.connector, time, datetime

# Constants for configuration
WORKSHEET_URL = 'https://docs.google.com/spreadsheets/d//edit#gid=0'
DATABASE_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': ''
}

def get_worksheet():
    """
    Function to get the Google Sheets worksheet.
    """
    try:
        gc = gspread.service_account(filename='static/token/linebotkey.json')
        worksheet = gc.open_by_url(WORKSHEET_URL).sheet1
        return worksheet
    except Exception as e:
        print(f"Error opening Google Sheets: {e}")
        return None

def connect_to_database():
    """
    Function to connect to the MySQL database.
    """
    try:
        cnx = mysql.connector.connect(**DATABASE_CONFIG)
        cursor = cnx.cursor()
        return cnx, cursor
    except mysql.connector.Error as err:
        print(f"Database connection error: {err}")
        return None, None

def process_rows(worksheet, cursor, cnx, formatted_date_time):
    """
    Function to process rows from the Google Sheet and insert them into the MySQL database.
    """
    rows = worksheet.get_all_records()

    for row in rows:
        if "NT-Equip" in str(row['Text']):
            query = "SELECT COUNT(*) FROM message_line WHERE DateLine = %s"
            values = (row['DateLine'],)
            cursor.execute(query, values)
            result = cursor.fetchone()
            count = result[0]
            if count == 0:
                def remove_text_before_NT_Equip(text):
                    index = text.find('NT-Equip')
                    if index != -1:
                        return text[index:]
                    else:
                        return text
                query = "INSERT INTO message_line (DateLine, DateExcel, replyToken, Text) VALUES (%s, %s, %s, %s)"
                values = (row['DateLine'], row['DateExcel'], row['replyToken'], remove_text_before_NT_Equip(row['Text']))
                cursor.execute(query, values)
                cnx.commit()
                message = f"{formatted_date_time} พบข้อมูลในระบบแล้วจำนวน {count}"
                print(message)

def main():
    """
    Main function to orchestrate the process.
    """
    while True:
        worksheet = get_worksheet()
        if worksheet is None:
            time.sleep(1000)
            continue

        cnx, cursor = connect_to_database()
        if cnx is None or cursor is None:
            time.sleep(1000)
            continue

        now = datetime.datetime.now()
        formatted_date_time = now.strftime("%d-%m-%Y %H:%M:%S")
        process_rows(worksheet, cursor, cnx, formatted_date_time)
        cnx.close()

        messwork = f"{formatted_date_time} ระบบกำลังทำงาน!"
        print(messwork)
        time.sleep(1000)

if __name__ == "__main__":
    main()