# Apache Iceberg Telecom Demo - Platform-Specific Versions

This repository contains three optimized versions of the Apache Iceberg telecom enterprise demo, each tailored for specific platforms to avoid compatibility conflicts.

## 📱 Choose Your Platform

### 🍎 macOS Version
**File:** `apache_iceberg_telecom_demo_mac.ipynb`

**Optimized for:**
- macOS systems (Intel/Apple Silicon)
- Local development environment
- Homebrew Java installations

**Features:**
- macOS-specific Java detection using `/usr/libexec/java_home`
- Homebrew installation instructions
- Local file paths optimized for macOS
- Moderate dataset size for laptop performance

**Requirements:**
- Java 8+ (`brew install openjdk@8`)
- Python 3.8+
- 4GB+ RAM recommended

---

### 🔬 Google Colab Version
**File:** `apache_iceberg_telecom_demo_colab.ipynb`

**Optimized for:**
- Google Colab environment
- Cloud-based execution
- No local setup required

**Features:**
- Colab-specific package installation (--no-deps flags)
- Uses Colab's pre-installed packages
- Smaller dataset optimized for free Colab resources
- Cloud-friendly memory configurations

**Requirements:**
- Google account
- Web browser
- No local installation needed

**Usage:**
1. Upload to Google Colab
2. Run all cells sequentially
3. Share with team members easily

---

### 🐧 Linux Version
**File:** `apache_iceberg_telecom_demo_linux.ipynb`

**Optimized for:**
- Linux production environments
- Ubuntu, CentOS, RHEL, Debian
- High-performance servers

**Features:**
- Multi-distribution Linux support
- Production-scale dataset (200+ sites, 100+ time chunks)
- Enhanced telecom metrics (8G technology, 4 vendors)
- Advanced Iceberg features (schema evolution, compaction)
- Enterprise-grade analytics

**Requirements:**
- Java 8+ (`sudo apt-get install openjdk-8-jdk` or `sudo yum install java-1.8.0-openjdk-devel`)
- Python 3.8+
- 8GB+ RAM recommended
- Significant disk space for large datasets

---

## 🚀 Quick Start

1. **Choose your platform** from the options above
2. **Download the appropriate notebook** for your environment
3. **Follow the setup instructions** in the first cell of the notebook
4. **Run cells sequentially** - each notebook is self-contained

## 🔧 Why Separate Versions?

The original cross-platform notebook was experiencing conflicts due to:
- Different Java installation paths across platforms
- Package dependency conflicts between environments
- Memory and resource constraints varying by platform
- Platform-specific optimization opportunities

Each version now provides:
- ✅ **Zero conflicts** - platform-specific setup
- ✅ **Optimized performance** - tuned for each environment
- ✅ **Clear instructions** - no guesswork
- ✅ **Reliable execution** - tested configurations

## 📊 Dataset Comparison

| Platform | Sites | Time Chunks | Technologies | Vendors | Focus |
|----------|-------|-------------|--------------|---------|-------|
| macOS    | 100   | 50          | 6G, 7G       | 3       | Development |
| Colab    | 50    | 20          | 6G, 7G       | 3       | Learning |
| Linux    | 200   | 100         | 6G, 7G, 8G   | 4       | Production |

## 🤝 Support

- **macOS issues**: Check Java installation with `brew list openjdk`
- **Colab issues**: Try "Runtime" → "Restart runtime" if packages conflict
- **Linux issues**: Verify Java with `java -version` and check distribution-specific commands

## 📝 License

Each notebook is self-contained and includes the same Apache Iceberg telecom enterprise demonstration with platform-optimized configurations. # iceberg_enterprise_tutorial
