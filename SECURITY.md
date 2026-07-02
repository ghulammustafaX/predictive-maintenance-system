# 🔒 Security Guidelines

## ⚠️ NEVER Commit These Files

### Sensitive Configuration
- `.env` files (all `.env.*` except `.env.example`)
- `client_secret_*.json` (Google OAuth credentials)
- Any files containing API keys, passwords, or tokens
- Database files (`.db`, `.sqlite`, `.sqlite3`)
- Private keys (`.pem`, `.key`, `.cert`)

### Large Data Files
- ML model files (`.pt`, `.pth`, `.pkl`, `.h5`)
- Dataset files (`.npz`, `.npy`, `.csv`, `.parquet`)
- Generated/processed data in `ml/saved/`
- Raw datasets in `simulator/data/cmapss/` and `simulator/data/ims/`

## ✅ Setup Checklist

1. **Copy environment templates:**
   ```bash
   cp .env.example .env
   cp backend/.env.example backend/.env
   ```

2. **Generate secure secrets:**
   ```bash
   python -c "import secrets; print(secrets.token_hex(32))"
   ```
   Use this for `JWT_SECRET` in your `.env` files

3. **Configure SMTP:** 
   - Use app-specific passwords for Gmail
   - Never use your actual email password

4. **Google OAuth Setup:**
   - Download client secret from Google Cloud Console
   - Save as `client_secret_*.json` (automatically gitignored)
   - Add Client ID to backend `.env`

## 🔍 Pre-Commit Checks

Before committing, verify:
```bash
# Check for accidentally staged sensitive files
git status

# Search for potential secrets in staged files
git diff --cached | grep -i "password\|secret\|token\|api_key"

# Verify .env files are ignored
git ls-files | grep "\.env$"  # Should return nothing
```

## 🛡️ Security Best Practices

1. **Never hardcode secrets** in source code
2. **Use environment variables** for all configuration
3. **Rotate secrets regularly**, especially after:
   - Team member departure
   - Suspected compromise
   - Public repository exposure
4. **Use different secrets** for development/staging/production
5. **Enable 2FA** on all service accounts (GitHub, Google Cloud, etc.)

## 🚨 If Secrets Are Exposed

1. **Immediately rotate** all exposed credentials
2. **Change passwords** on affected services
3. **Revoke tokens** and generate new ones
4. **Remove from git history** using:
   ```bash
   git filter-branch --force --index-filter \
   "git rm --cached --ignore-unmatch PATH/TO/FILE" \
   --prune-empty --tag-name-filter cat -- --all
   ```
5. **Force push** after cleaning history (dangerous - coordinate with team)

## 📋 What's Safe to Commit

✅ Code files (`.py`, `.js`, `.html`, `.css`)
✅ Configuration templates (`.env.example`)
✅ Requirements files (`requirements.txt`, `package.json`)
✅ Documentation (`.md` files)
✅ Small sample data for testing
✅ Docker configuration files

## 🔗 Resources

- [GitHub Secret Scanning](https://docs.github.com/en/code-security/secret-scanning)
- [Git-secrets tool](https://github.com/awslabs/git-secrets)
- [Python Secrets module](https://docs.python.org/3/library/secrets.html)
