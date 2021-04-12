import csv
import threading

import click
import imaplib
import logging
import re
from email.parser import BytesHeaderParser
from collections import Counter


logger = logging.getLogger(__name__)


class AtomicCounter:
    def __init__(self):
        self._lock = threading.Lock()
        self.counter = Counter()

    def increment(self, value):
        with self._lock:
            self.counter[value] += 1

    def most_common(self, num=None):
        return self.counter.most_common(n=num)


class EmailCounter:
    def __init__(
        self, username, password, imap_server, mailbox, counter, num_chunks=10
    ):
        self.username = username
        self.password = password
        self.mailbox = mailbox
        self.counter = counter
        self.num_chunks_size = num_chunks
        self.M = imaplib.IMAP4_SSL(imap_server)
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

    def list_mailboxes(self):
        for m in self.M.list()[1]:
            yield m

    def select_mailbox(self, mailbox):
        self.M.select(mailbox)

    def get_ids_chunks(self, num_emails=None):
        self.select_mailbox(self.mailbox)
        type, data = self.M.search(None, "ALL")
        ids = data[0].split()
        if num_emails:
            ids = ids[-num_emails:]

        chunk_size = round(len(ids) / self.num_chunks_size)

        for i in range(0, len(ids), chunk_size):
            yield ids[i : i + chunk_size]

    def count_mc_addresses(self, chunk):
        self.select_mailbox(self.mailbox)
        for id in chunk:
            typ, data = self.M.fetch(id, "(BODY.PEEK[HEADER])")
            logger.warning("Processing {}".format(id))
            h = self.parser.parsebytes(data[0][1])
            try:
                email = self.email_re.search(h["To"]).group(1)
                self.counter.increment(email)
            except (AttributeError, TypeError):
                logger.error(h["To"])
                pass


def count_chunk(username, password, imap_server, mailbox, counter, chunk):
    with EmailCounter(username, password, imap_server, mailbox, counter) as EC:
        EC.count_mc_addresses(chunk)


@click.command()
@click.option(
    "--username", prompt="Your username", help="The username of the email account."
)
@click.option(
    "--password", prompt="Your password", help="The password of the email account."
)
@click.option(
    "--imap_server",
    prompt="Imap server address",
    help="The address of the imap server.",
)
@click.option("--num_emails", default=50000, help="Number of emails to retrieve.")
@click.option(
    "--num_addresses", default=1000, help="Number of most common addresses to return."
)
@click.option("--output", default="output.csv", help="Output file.")
def main(username, password, imap_server, num_emails, num_addresses, output):
    counter = AtomicCounter()
    mailbox = '"[Gmail]/Sent Mail"'
    with EmailCounter(username, password, imap_server, mailbox, counter) as EC:
        for m in EC.list_mailboxes():
            print(m)
        chunks = EC.get_ids_chunks(num_emails)
        threads = []
        for chunk in chunks:
            thread = threading.Thread(
                target=count_chunk,
                args=(
                    username,
                    password,
                    imap_server,
                    mailbox,
                    counter,
                    chunk,
                ),
            )
            thread.start()
            threads.append(thread)
    for thread in threads:
        thread.join()
    most_common_emails = counter.most_common(num=num_addresses)

    with open(output, "w+") as f:
        writer = csv.writer(f)
        writer.writerow(("email", "count"))
        for row in most_common_emails:
            writer.writerow(row)


if __name__ == "__main__":
    main()
