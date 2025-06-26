# Apache Iceberg Telecom Enterprise Demo

A comprehensive collection of Jupyter notebooks demonstrating Apache Iceberg for telecom enterprise environments across multiple platforms.

## 📋 Overview

This repository contains platform-specific notebooks that showcase Apache Iceberg's capabilities for telecom data management:

- **ACID transactions** for reliable telecom data operations
- **Schema evolution** for adapting to new network technologies  
- **Time travel** for historical network performance analysis
- **Hidden partitioning** for optimal time-series data queries
- **Data compaction** for efficient storage of large telecom datasets

## 🖥️ Platform-Specific Notebooks

### 1. **macOS Version** - `apache_iceberg_telecom_demo_mac.ipynb`
- **Target**: macOS developers and data engineers
- **Features**: Homebrew integration, macOS-specific Java detection
- **Dataset**: 100 sites, 50 time chunks (6G/7G, 3 vendors)
- **Best for**: Local development and testing on macOS

### 2. **Linux Version** - `apache_iceberg_telecom_demo_linux.ipynb`  
- **Target**: Production Linux environments
- **Features**: Multi-distribution support (Ubuntu, CentOS, RHEL, Debian)
- **Dataset**: 200 sites, 100 time chunks (6G/7G/8G, 4 vendors)
- **Best for**: Production deployments and enterprise Linux servers

### 3. **Windows Version** - `apache_iceberg_telecom_demo_windows.ipynb`
- **Target**: Windows enterprise environments
- **Features**: Windows Registry Java detection, PowerShell/CMD compatibility
- **Dataset**: 150 sites, 75 time chunks (6G/7G/8G, 4 vendors)
- **Best for**: Windows-based enterprise deployments

### 4. **Google Colab Version** - `apache_iceberg_telecom_demo_colab_final.ipynb`
- **Target**: Cloud collaboration and learning
- **Features**: No setup required, built-in sharing, GPU access
- **Dataset**: 50 sites, 20 time chunks (optimized for free Colab resources)
- **Best for**: Learning, prototyping, and team collaboration

## 🚀 Quick Start

### Choose Your Platform:

**For macOS:**
```bash
git clone <repository-url>
cd iceberg_tutorials
jupyter notebook apache_iceberg_telecom_demo_mac.ipynb
```

**For Linux:**
```bash
git clone <repository-url>
cd iceberg_tutorials
jupyter notebook apache_iceberg_telecom_demo_linux.ipynb
```

**For Windows:**
```bash
git clone <repository-url>
cd iceberg_tutorials
jupyter notebook apache_iceberg_telecom_demo_windows.ipynb
```

**For Google Colab:**
1. Open [Google Colab](https://colab.research.google.com/)
2. Upload `apache_iceberg_telecom_demo_colab_final.ipynb`
3. Run all cells

## 📊 What's Included

Each notebook demonstrates:

1. **Platform-Specific Setup** - Automated environment configuration
2. **Telecom Data Generation** - Realistic network performance metrics
3. **Iceberg Table Creation** - Time-partitioned tables for optimal queries
4. **Basic Operations** - Schema exploration and data analysis
5. **Time Travel** - Historical data recovery and comparison
6. **Schema Evolution** - Adding columns without breaking compatibility
7. **Advanced Analytics** - Performance optimization and best practices

## 🏗️ Telecom Data Model

### Network Infrastructure:
- **Sites**: 50-200 telecom sites across multiple regions
- **Technologies**: 6G, 7G, 8G networks
- **Vendors**: Ericia, Noson, Weihu, Samsong
- **Regions**: North, South, East, West, Central

### Performance Metrics:
- **RSSI** (Received Signal Strength Indicator)
- **Latency** measurements in milliseconds
- **Data Volume** in megabytes
- **Drop Rate** percentages
- **CPU Usage** percentages
- **Signal Quality** categories
- **Network Reliability** indices

## 🔧 Requirements

### All Platforms:
- Java 8+ (automatically detected/installed)
- Python 3.8+
- 4GB+ available RAM
- 2GB+ disk space

### Platform-Specific:
- **macOS**: Homebrew (optional but recommended)
- **Linux**: Package manager (apt, yum, etc.)
- **Windows**: PowerShell or Command Prompt
- **Colab**: Google account (no local setup required)

## 🌟 Key Features by Platform

| Feature | macOS | Linux | Windows | Colab |
|---------|-------|-------|---------|-------|
| Auto Java Detection | ✅ | ✅ | ✅ | ✅ |
| Package Management | Homebrew | apt/yum/dnf | Chocolatey/winget | Pre-installed |
| Dataset Size | Medium | Large | Medium | Small |
| Production Ready | ✅ | ✅ | ✅ | ❌ |
| Collaboration | ❌ | ❌ | ❌ | ✅ |
| No Setup Required | ❌ | ❌ | ❌ | ✅ |

## 📚 Enterprise Use Cases

### Telecom Network Operations:
- Real-time network performance monitoring
- Historical trend analysis and capacity planning
- Vendor performance comparison
- Technology migration planning (6G → 7G → 8G)

### Data Engineering:
- Time-series data lake architecture
- Schema evolution for new metrics
- Data retention and lifecycle management
- Cross-platform deployment strategies

## 🔗 Sharing and Collaboration

### Google Colab Sharing:
- **Direct Link**: Share notebook URL with view/edit permissions
- **GitHub Integration**: Push to GitHub for team access
- **Google Drive**: Share via team folders
- **Download Options**: Export as .ipynb, .py, or .html

### Enterprise Deployment:
- Version control with Git
- CI/CD pipeline integration
- Container deployment (Docker/Kubernetes)
- Cloud platform deployment (AWS, Azure, GCP)

## 📖 Learning Path

1. **Start with Colab** - No setup, immediate hands-on experience
2. **Try Local Platform** - Run on your preferred OS (macOS/Linux/Windows)
3. **Explore Advanced Features** - Time travel, schema evolution, analytics
4. **Scale to Production** - Deploy on enterprise infrastructure

## 🛠️ Troubleshooting

### Common Issues:
- **Java not found**: Install OpenJDK from [Adoptium](https://adoptium.net/)
- **Memory errors**: Increase Spark driver memory configuration
- **Package conflicts**: Use virtual environments (conda/venv)
- **Network issues**: Check firewall settings for Spark ports

### Platform-Specific:
- **macOS**: Use `brew install openjdk` for easy Java installation
- **Linux**: Use distribution package manager for dependencies
- **Windows**: Configure Windows Defender exclusions for Spark
- **Colab**: Restart runtime if package installation fails

## 🤝 Contributing

We welcome contributions! Please:
1. Fork the repository
2. Create a feature branch
3. Test on your target platform
4. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📞 Support

For questions and support:
- Open an issue on GitHub
- Check the troubleshooting section
- Review platform-specific documentation

---

**Ready to explore Apache Iceberg for telecom enterprises?** Choose your platform and get started! 🚀
