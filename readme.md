This is a quick script that parses a vsftpd log file and looks for file upload lines. It logs those file upload entries to a Postgres database. It compares log entry dates so that it avoids writing duplicate entries. This is intended to be run as a cron job.

If you are actually interested in using this, then I am sure that the code is simple enough to be self explanitory. I am also sure that there are a million edge cases.

## Usage

```
python parser.py <connection-string> <logfile>
```