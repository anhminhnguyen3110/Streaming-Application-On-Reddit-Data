from email.mime.text import MIMEText
import xlwt
import json
from kafka import KafkaConsumer
import schedule
import time
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

topic_list = ["Negative_Submissions", "Negative_Comments", "Toxic_Comments"]
consumer = KafkaConsumer(
    *topic_list,
    bootstrap_servers=["host.docker.internal:29092"],
    auto_offset_reset="latest",
    value_deserializer=lambda x: x.decode("utf-8"),
)


def process_kafka_messages():
    start_time = time.time()
    end_time = start_time + 180
    output_dir = "excel"
    os.makedirs(output_dir, exist_ok=True)
    submissions_data = []
    comments_data = []
    toxic_data = []

    for message in consumer:
        data = json.loads(message.value)

        if message.topic == "Negative_Submissions":
            print("Message coming from Negative_Submissions")
            submissions_data.append(data)
        elif message.topic == "Negative_Comments":
            print("Message coming from Negative_Comments")
            comments_data.append(data)
        elif message.topic == "Toxic_Comments":
            print("Message coming from Toxic_Comments")
            toxic_data.append(data)

        if time.time() >= end_time:
            break

    file_name = f"negative_reddit_{str(int(time.time()))}.xls"
    excel_file = os.path.join(output_dir, file_name)
    workbook = xlwt.Workbook()
    if submissions_data:
        submissions_sheet = workbook.add_sheet("Submissions")

        headers = list(submissions_data[0].keys())

        for col_index, header in enumerate(headers):
            submissions_sheet.write(0, col_index, header)

        for row_index, data in enumerate(submissions_data, start=1):
            for col_index, header in enumerate(headers):
                submissions_sheet.write(row_index, col_index, data[header])

    if comments_data:
        comments_sheet = workbook.add_sheet("Comments")

        headers = list(comments_data[0].keys())

        for col_index, header in enumerate(headers):
            comments_sheet.write(0, col_index, header)

        for row_index, data in enumerate(comments_data, start=1):
            for col_index, header in enumerate(headers):
                comments_sheet.write(row_index, col_index, data[header])

    workbook.save(excel_file)

    if toxic_data:
        toxic_sheet = workbook.add_sheet("Toxic_Comments")

        headers = list(toxic_data[0].keys())

        for col_index, header in enumerate(headers):
            toxic_sheet.write(0, col_index, header)

        for row_index, data in enumerate(toxic_data, start=1):
            for col_index, header in enumerate(headers):
                toxic_sheet.write(row_index, col_index, data[header])

    print(f"Excel file saved as {excel_file}")
    print()
    send_mail(excel_file)


def send_mail(excel_file):
    smtp_server = "smtp.gmail.com"
    smtp_port = 465
    sender_email = "nganhminh2000@gmail.com"
    sender_password = "weah xtgs iduh pmuk"
    receiver_email = "nganhminh2000@gmail.com"

    with open("template/email.html", "r") as template_file:
        html_content = template_file.read()

    # Create the email
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = "Email from Minh Anh, Negative Comments and Submissions"

    msg.attach(MIMEText(html_content, "html"))

    with open(excel_file, "rb") as f:
        attachment = MIMEApplication(f.read(), _subtype="xls")
    attachment.add_header("Content-Disposition", "attachment", filename=excel_file)
    msg.attach(attachment)

    try:
        print("Sending email...")
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
        print("Email sent successfully")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        print()
        print(f"Waiting for next schedule...")
        print()


flag = True
if flag:
    process_kafka_messages()
    flag = False
schedule.every(3).minutes.do(process_kafka_messages)

while True:
    schedule.run_pending()
    time.sleep(1)

# send_mail('excel/output_1696419378.xls')
