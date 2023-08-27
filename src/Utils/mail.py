import smtplib
from email.mime.text import MIMEText

def send_mail(subject, content):
    msg_from='1164616594@qq.com'                           
    passwd='hiotjthalcxmbace'
    msg_to='wzhzhu@mail.ustc.edu.cn'

    msg = MIMEText(content)
    msg['Subject'] = subject
    msg['From'] = msg_from
    msg['To'] = msg_to
    try:
        s = smtplib.SMTP_SSL("smtp.qq.com",465)
        s.login(msg_from, passwd)
        s.sendmail(msg_from, msg_to, msg.as_string())
        print("sending success")
    except s.SMTPException as e:
        print("sending failed")
    finally:
        s.quit()