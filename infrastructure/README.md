# Databricks Lakeflow Jobs Infrastructure

This project demonstrates how to provision Databricks infrastructure on AWS using [**StackQL**](https://github.com/stackql/stackql) and [**stackql-deploy**](https://stackql-deploy.io/) as a modern, declarative, state-file less alternative to Terraform.

## About StackQL and stackql-deploy

**StackQL** is an open-source DevOps framework that uses SQL to query, provision, and manage cloud and SaaS resources. It provides a unified interface to interact with cloud providers through their APIs using familiar SQL syntax.

**stackql-deploy** is an Infrastructure as Code (IaC) framework built on top of StackQL that enables:
- **Declarative infrastructure management** using SQL-based templates
- **Multi-cloud support** with consistent syntax across providers
- **GitOps workflows** with native CI/CD integration
- **State management** without external state files
- **Real-time querying** of cloud resources alongside provisioning

### Why Choose StackQL over Terraform?

- **No proprietary DSL**: Use SQL, a language most developers already know
- **No state files**: Infrastructure state is queried directly from cloud APIs
- **Real-time visibility**: Query your infrastructure state at any time
- **Unified interface**: Manage multiple cloud providers with consistent syntax
- **Lightweight**: No agents or servers required

## What This Project Provisions

This infrastructure stack creates:
- AWS IAM roles and policies for cross-account access
- S3 buckets with appropriate policies for Databricks workspace storage
- Databricks workspace with Unity Catalog metastore
- Storage credentials and external locations for data access
- Network configurations and security groups

## Prerequisites

Before starting, ensure you have:

### AWS Account Setup
- An active AWS account with administrative permissions
- AWS CLI configured with appropriate credentials

### Databricks Account Setup
Required manual setup steps (one-time only):
1. **Enable Databricks on AWS Marketplace**: Visit the [AWS Marketplace subscriptions page](https://console.aws.amazon.com/marketplace/home#/subscriptions) and subscribe to the Databricks offering
2. **Complete Databricks initial setup**: Follow the guided setup flow to create your Databricks account
3. **Collect account identifiers**:
   - **Databricks Account ID**: Available in the Databricks console (click user icon → copy account ID)
   - **Databricks AWS Account ID**: Found in the cross-account role or storage configuration (different from your main AWS account)
4. **Create service principal for CI/CD**:
   - Go to Databricks Account Console → User Management → Service Principals
   - Create a new service principal
   - Grant it Account Admin role
   - Generate a client secret and securely store both client ID and secret

### Local Development Setup
- Python 3.8+ with virtual environment support
- Git for version control

## Environment Variables

Create a `.env` file or set the following environment variables:

```bash
# AWS Configuration
export AWS_REGION='us-east-1'                    # Your preferred AWS region
export AWS_ACCOUNT_ID='123456789012'             # Your AWS account ID
export AWS_ACCESS_KEY_ID='your-aws-access-key'   # AWS credentials (optional if using AWS CLI/IAM roles)
export AWS_SECRET_ACCESS_KEY='your-aws-secret'   # AWS credentials (optional if using AWS CLI/IAM roles)

# Databricks Configuration
export DATABRICKS_ACCOUNT_ID='your-databricks-account-id'        # From Databricks console
export DATABRICKS_AWS_ACCOUNT_ID='databricks-aws-account-id'     # From Databricks cross-account setup
export DATABRICKS_CLIENT_ID='your-service-principal-client-id'   # Service principal client ID
export DATABRICKS_CLIENT_SECRET='your-service-principal-secret'  # Service principal secret
```

**Security Note**: Never commit these values to version control. Use environment variables, CI/CD secrets, or secure credential management systems.

## `stackql-deploy` Commands and Arguments Overview

`stackql-deploy` provides a simple yet powerful command-line interface for managing infrastructure lifecycles. The tool follows a consistent pattern:

```bash
stackql-deploy <command> <stack_dir> <stack_env> [options]
```

### Core Commands

- **`build`**: Deploy/provision infrastructure resources
- **`test`**: Validate that deployed infrastructure matches expected state
- **`teardown`**: Destroy all provisioned resources
- **`info`**: Display information about the stack configuration

### Positional Arguments

1. **`stack_dir`**: Path to the directory containing your `stackql_manifest.yml` and resource templates
   - Example: `infrastructure` (refers to the `./infrastructure/` directory)
   
2. **`stack_env`**: Target environment for deployment
   - Example: `dev`, `staging`, `prod`
   - The same codebase can target multiple environments by making environment-specific substitutions
   - Environment variables and configurations are interpolated based on this value

### Key Features

**Multi-Environment Support**: Use the same infrastructure code across different environments by leveraging the `stack_env` parameter. StackQL-deploy automatically substitutes environment-specific values in your templates, allowing you to:
- Deploy to `dev` environment with smaller instance sizes
- Use `prod` environment with production-grade configurations  
- Maintain separate naming conventions per environment (e.g., `myapp-dev-bucket` vs `myapp-prod-bucket`)

**Common Options**:
- `--dry-run`: Preview what would be deployed without making changes
- `--show-queries`: Display the actual SQL queries being executed
- `-e KEY=VALUE`: Pass environment variables for template substitution
- `--log-level`: Control verbosity of output

## Local Deployment

### 1. Setup Environment

```bash
# Create and activate virtual environment
python3 -m venv myenv
source myenv/bin/activate  # On Windows: myenv\Scripts\activate

# Install stackql-deploy
pip install stackql-deploy
```

### 2. Validate Configuration (Dry Run)

Before deploying, run a dry run to validate your configuration:

```bash
stackql-deploy build \
infrastructure dev \
-e AWS_REGION=${AWS_REGION} \
-e AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID} \
-e DATABRICKS_ACCOUNT_ID=${DATABRICKS_ACCOUNT_ID} \
-e DATABRICKS_AWS_ACCOUNT_ID=${DATABRICKS_AWS_ACCOUNT_ID} \
--dry-run
```

### 3. Deploy Infrastructure

Deploy the full infrastructure stack:

```bash
stackql-deploy build \
infrastructure dev \
-e AWS_REGION=${AWS_REGION} \
-e AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID} \
-e DATABRICKS_ACCOUNT_ID=${DATABRICKS_ACCOUNT_ID} \
-e DATABRICKS_AWS_ACCOUNT_ID=${DATABRICKS_AWS_ACCOUNT_ID} \
--show-queries
```

### 4. Test Deployment

Verify the infrastructure is correctly provisioned:

```bash
stackql-deploy test \
infrastructure dev \
-e AWS_REGION=${AWS_REGION} \
-e AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID} \
-e DATABRICKS_ACCOUNT_ID=${DATABRICKS_ACCOUNT_ID} \
-e DATABRICKS_AWS_ACCOUNT_ID=${DATABRICKS_AWS_ACCOUNT_ID} \
--show-queries
```

### 5. Teardown (When Done)

Clean up all provisioned resources:

```bash
stackql-deploy teardown \
infrastructure dev \
-e AWS_REGION=${AWS_REGION} \
-e AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID} \
-e DATABRICKS_ACCOUNT_ID=${DATABRICKS_ACCOUNT_ID} \
-e DATABRICKS_AWS_ACCOUNT_ID=${DATABRICKS_AWS_ACCOUNT_ID} \
--show-queries
```

## CI/CD with GitHub Actions

This repository includes a GitHub Actions workflow (`.github/workflows/databricks-dab.yml`) that demonstrates automated infrastructure provisioning using the [**stackql-deploy GitHub Action**](https://github.com/marketplace/actions/stackql-deploy).

### GitHub Secrets Configuration

To use the automated workflow, configure the following secrets in your GitHub repository:

```
AWS_ACCESS_KEY_ID              # AWS access key for deployment
AWS_SECRET_ACCESS_KEY          # AWS secret key for deployment  
AWS_REGION                     # AWS region (e.g., us-east-1)
AWS_ACCOUNT_ID                 # Your AWS account ID
DATABRICKS_ACCOUNT_ID          # Your Databricks account ID
DATABRICKS_AWS_ACCOUNT_ID      # Databricks cross-account AWS ID
DATABRICKS_CLIENT_ID           # Service principal client ID
DATABRICKS_CLIENT_SECRET       # Service principal secret
```

### Workflow Features

The GitHub Actions workflow:
- **Triggers** on pushes to `main` and pull requests affecting infrastructure or job configurations
- **Provisions infrastructure** using the `stackql/stackql-deploy-action@v1.0.2`
- **Validates Databricks Asset Bundles** for the retail job configurations
- **Deploys and tests** the complete data pipeline
- **Provides detailed outputs** including workspace URLs and deployment status

### Key Workflow Steps

1. **Environment Detection**: Automatically determines deployment environment (dev for PRs, prd for main branch)
2. **StackQL Setup**: Installs StackQL and pulls required Databricks providers
3. **Infrastructure Deployment**: Uses stackql-deploy to provision all AWS and Databricks resources
4. **Workspace Configuration**: Parses deployment outputs and configures workspace access
5. **DAB Validation & Deployment**: Validates and deploys Databricks Asset Bundles for data jobs

### Using the stackql-deploy GitHub Action

The workflow demonstrates the stackql-deploy GitHub Action usage:

```yaml
- name: Deploy Infrastructure with StackQL
  uses: stackql/stackql-deploy-action@v1.0.2
  with:
    command: 'build'
    stack_dir: 'infrastructure'
    stack_env: ${{ steps.set-env.outputs.stack_env }}
    env_vars: AWS_REGION=${{ secrets.AWS_REGION }},AWS_ACCOUNT_ID=${{ secrets.AWS_ACCOUNT_ID }},DATABRICKS_ACCOUNT_ID=${{ secrets.DATABRICKS_ACCOUNT_ID }},DATABRICKS_AWS_ACCOUNT_ID=${{ secrets.DATABRICKS_AWS_ACCOUNT_ID }}
```

This action provides:
- **Simplified CI/CD integration** for StackQL deployments
- **Automatic environment management** and credential handling
- **Detailed logging** and deployment summaries
- **Error handling** and rollback capabilities

## Optional: Manual StackQL Verification

You can manually verify your deployment using the StackQL interactive shell:

```bash
# Start StackQL shell
stackql shell

# Pull required providers
REGISTRY PULL databricks_account;
REGISTRY PULL databricks_workspace;

# Verify account access
SELECT account_id 
FROM databricks_account.provisioning.credentials 
WHERE account_id = 'your-databricks-account-id';

# List workspaces
SELECT account_id, workspace_name, workspace_id, workspace_status 
FROM databricks_account.provisioning.workspaces 
WHERE account_id = 'your-databricks-account-id';
```

## Project Structure

```
infrastructure/
├── README.md                           # This file
├── stackql_manifest.yml               # StackQL deployment configuration
└── resources/                          # StackQL resource templates
    ├── aws/
    │   ├── iam/                       # IAM roles and policies
    │   └── s3/                        # S3 buckets and policies
    ├── databricks_account/            # Account-level Databricks resources
    └── databricks_workspace/          # Workspace-level configurations
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify all environment variables are set correctly
   - Ensure service principal has Account Admin permissions
   - Check AWS credentials have sufficient permissions

2. **Deployment Failures**
   - Run with `--dry-run` first to validate configuration
   - Check that Databricks marketplace subscription is active
   - Verify account IDs are correct (AWS vs Databricks AWS account)

3. **Resource Conflicts**
   - Ensure resource names are unique
   - Check for existing resources with same names
   - Verify region consistency across configurations

### Getting Help

- **StackQL Documentation**: [https://stackql.io/docs](https://stackql.io/docs)
- **stackql-deploy Guide**: [https://github.com/stackql/stackql-deploy](https://github.com/stackql/stackql-deploy)
- **Databricks on AWS Setup**: [Databricks AWS Documentation](https://docs.databricks.com/en/getting-started/overview.html)

## Cancel Databricks Subscription

⚠️ **IMPORTANT**: After completing this exercise, you **MUST** cancel your Databricks subscription to avoid ongoing charges.

1. Go to the [AWS Marketplace manage subscriptions page](https://console.aws.amazon.com/marketplace/home#/subscriptions)
2. Navigate to the Databricks subscription
3. Cancel the subscription
4. Verify all AWS resources have been destroyed using the AWS Console

## Additional Resources

- **StackQL Official Website**: [https://stackql.io](https://stackql.io)
- **StackQL GitHub**: [https://github.com/stackql/stackql](https://github.com/stackql/stackql)  
- **stackql-deploy GitHub Action**: [https://github.com/stackql/stackql-deploy-action](https://github.com/stackql/stackql-deploy-action)
- **Databricks Documentation**: [https://docs.databricks.com](https://docs.databricks.com)

---

*This project demonstrates the power of SQL-based infrastructure as code with StackQL. For questions or contributions, please visit our GitHub repository.*

