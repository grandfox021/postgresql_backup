# 🐘 PostgreSQL Backup Script

A powerful and fully automated Python script for creating compressed backups of multiple PostgreSQL databases across one or more servers — with progress logging, cleanup, and environment-based configuration.

---

## 🚀 Features

- ✅ **Supports multiple servers and databases**  
- 💾 **Creates compressed `.tar.gz` backups**  
- 🧩 **Reads configuration from `.env` file only (no code edits required)**  
- 📊 **Live progress logging for each backup**  
- 🧹 **Automatic cleanup of old backups and logs (based on retention days)**  
- ⚙️ **Works with `pg_dump` using custom user/password per database**
- uses maximum compression level (level 9) !

