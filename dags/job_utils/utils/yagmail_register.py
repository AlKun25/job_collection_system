import os
import yagmail
from dotenv import load_dotenv

load_dotenv()

def yagmail_register():
    yag = yagmail.SMTP(user=os.getenv("SENDER_USERNAME"), oauth2_file=os.getenv("SENDER_OAUTH2_FILE"))
    print("Yagmail registered successfully", yag.user)

def yagmail_test_mail():
    yag = yagmail.SMTP(user=os.getenv("SENDER_USERNAME"), oauth2_file=os.getenv("SENDER_OAUTH2_FILE"))
    try:
        yag.send(
            to=os.getenv("RECEIVER_EMAIL"),
            subject="Yagmail Test",
            contents="This is a test email sent using Yagmail.")
        print("Email sent successfully")
    except Exception as e:
        print(f"Error sending email: {e}")

if __name__ == "__main__":
    yagmail_test_mail()