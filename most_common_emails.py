import csv
import click
import imaplib
import logging
import re
from email.parser import BytesHeaderParser
from collections import Counter


logger = logging.getLogger(__name__)


class EmailCounter:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.M = imaplib.IMAP4_SSL("imap.gmail.com")
        self.parser = BytesHeaderParser()
        self.email_re = re.compile(r"([\w\.-]+@[\w\.-]+)")

    def __enter__(self):
        self.login()
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.logout()

    def login(self):
        self.M.login(self.username, self.password)

    def logout(self):
        self.M.close()
        self.M.logout()

    def count_mc_addresses(self, mailbox, num_emails=1000, num_addresses=10):
        self.M.select(mailbox)
        type, data = self.M.search(None, "ALL")
        ids = data[0].split()
        c = Counter()
        for i in ids[-num_emails:]:
            typ, data = self.M.fetch(i, "(BODY.PEEK[HEADER])")
            logger.warning("Processing {}".format(i))
            h = self.parser.parsebytes(data[0][1])
            try:
                c[self.email_re.search(h["To"]).group(1)] += 1
            except AttributeError:
                logger.error(h["To"])
        return c.most_common(n=num_addresses)


@click.command()
@click.option(
    "--username", prompt="Your username", help="The username of the email account."
)
@click.option(
    "--password", prompt="Your password", help="The password of the email account."
)
@click.option(
    "--num_emails", default=50000, help="Number of emails to retrieve."
)
@click.option(
    "--num_addresses", default = 1000, help="Number of most common addresses to return."
)
@click.option(
    "--output", default="output.csv", help="Output file."
)
def main(username, password, num_emails, num_addresses, output):
    with EmailCounter(username, password) as EC:
        most_common = EC.count_mc_addresses(
            '"[Gmail]/Sent Mail"', num_emails=num_emails, num_addresses=num_addresses
        )

    with open(output, "w+") as f:
        writer=csv.writer(f)
        writer.writerow(("email", "count"))
        for row in most_common:
            writer.writerow(row)


if __name__ == "__main__":
    main()
