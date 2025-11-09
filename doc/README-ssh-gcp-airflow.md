# SSH Key Management for GCP Airflow

## Updating Existing SSH Key

### Step 1: Backup Existing Key (Optional)
```bash
# Backup existing key
cp ~/.ssh/gcp_key ~/.ssh/gcp_key.backup
cp ~/.ssh/gcp_key.pub ~/.ssh/gcp_key.pub.backup
```

### Step 2: Generate New Key with Same Name
```bash
# Generate new SSH key with the same name (overwrites existing)
ssh-keygen -t rsa -b 4096 -f ~/.ssh/gcp_key -C ahsnl_mi

# When prompted:
# - Press Enter for no passphrase (or enter a secure passphrase)
# - Press Enter again to confirm
# - Type 'y' to overwrite the existing key
```

### Step 3: View Your New Public Key
```bash
# Display your new public key
cat ~/.ssh/gcp_key.pub
```

### Step 4: Add New Public Key to Server

1. **Go to Google Cloud Console**
2. **Navigate to**: Compute Engine → VM instances
3. **Find your instance** (Example IP: 34.141.47.135)
4. **Click "SSH"** to open browser terminal
5. **Replace the old key with the new one**:
   ```bash
   # Remove old key (if it exists)
   sed -i '/ahsnl_mi@gmail.com/d' ~/.ssh/authorized_keys
   
   # Add new key
   echo 'YOUR_NEW_PUBLIC_KEY_HERE' >> ~/.ssh/authorized_keys

   # Check if key is added
   cat ~/.ssh/authorized_keys | grep ahsnl_mi
   
   # Set correct permissions
   chmod 600 ~/.ssh/authorized_keys
   chmod 700 ~/.ssh
   ```

### Step 5: Test Connection
```bash
# Test SSH connection
ssh gcp-airflow 'echo "SSH connection successful!"'
```
```