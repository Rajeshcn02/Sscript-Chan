import requests
import csv
import os
import logging
from dotenv import load_dotenv
from time import sleep

# Load GitHub tokens, server URL, and organization names from .env file
load_dotenv()

# Provide default values in case environment variables are not set
GITHUB_TOKENS = [token.strip() for token in os.getenv("GITHUB_TOKENS", "").split(",") if token.strip()]
GITHUB_SERVER_URL = os.getenv("GITHUB_SERVER_URL", "").strip()
ORG_NAMES = [org.strip() for org in os.getenv("ORG_NAMES", "").split(",") if org.strip()]

# Validate required environment variables
if not GITHUB_TOKENS:
    raise ValueError("GITHUB_TOKENS is missing or empty in the .env file.")
if not GITHUB_SERVER_URL:
    raise ValueError("GITHUB_SERVER_URL is missing or empty in the .env file.")
if not ORG_NAMES:
    raise ValueError("ORG_NAMES is missing or empty in the .env file.")

# GitHub Enterprise Server GraphQL API endpoint
API_URL = f"{GITHUB_SERVER_URL}/graphql"

# Set up logging for errors
logging.basicConfig(filename="github_repo_fetch_errors.log", 
                    level=logging.ERROR, 
                    format="%(asctime)s:%(levelname)s:%(message)s")

# Track the current token index
token_index = 0

def get_headers():
    """Function to get headers with the current token."""
    global token_index
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKENS[token_index]}"
    }
    return headers

def switch_token():
    """Switch to the next token in the list."""
    global token_index
    token_index = (token_index + 1) % len(GITHUB_TOKENS)
    logging.info(f"Switched to token index: {token_index}")
    print(f"Switched to token index: {token_index}")

def fetch_pull_requests_count(repo_name, org_name):
    """Fetch all pull requests for a repository and count open and closed (including merged) ones."""
    open_count = 0
    closed_count = 0
    cursor = None
    query = """
    query($org: String!, $repo: String!, $cursor: String) {
      repository(owner: $org, name: $repo) {
        pullRequests(first: 100, after: $cursor) {
          nodes {
            state
          }
          pageInfo {
            endCursor
            hasNextPage
          }
        }
      }
    }
    """
    
    while True:
        variables = {"org": org_name, "repo": repo_name, "cursor": cursor}
        try:
            response = requests.post(API_URL, json={"query": query, "variables": variables}, headers=get_headers(), timeout=10)
            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                logging.error(f"GraphQL errors for {repo_name} in {org_name}: {data['errors']}")
                print(f"GraphQL errors for {repo_name} in {org_name}: {data['errors']}")
                switch_token()
                continue

            pull_requests = data["data"]["repository"]["pullRequests"]["nodes"]
            for pr in pull_requests:
                if pr["state"] == "CLOSED" or pr["state"] == "MERGED":
                    closed_count += 1
                elif pr["state"] == "OPEN":
                    open_count += 1

            print(f"Fetched page for {repo_name}. Open PRs: {open_count}, Closed PRs (incl. merged): {closed_count}")

            if not data["data"]["repository"]["pullRequests"]["pageInfo"]["hasNextPage"]:
                break
            cursor = data["data"]["repository"]["pullRequests"]["pageInfo"]["endCursor"]
            sleep(1)  # To respect rate limits

        except requests.exceptions.RequestException as e:
            logging.error(f"Network error for repository {repo_name} in organization {org_name}: {e}")
            print(f"Network error for repository {repo_name} in organization {org_name}: {e}")
            switch_token()
        except Exception as e:
            logging.error(f"Unexpected error for repository {repo_name} in organization {org_name}: {e}")
            print(f"Unexpected error for repository {repo_name} in organization {org_name}: {e}")
            switch_token()

    return open_count, closed_count

def fetch_org_repos(org_name):
    print(f"Fetching repositories for organization: {org_name}")
    query = """
    query($org: String!, $cursor: String) {
      organization(login: $org) {
        repositories(first: 100, after: $cursor) {
          pageInfo {
            endCursor
            hasNextPage
          }
          nodes {
            name
            isPrivate
            diskUsage
            defaultBranchRef {
              name
              target {
                ... on Commit {
                  history(first: 1) {
                    nodes {
                      author {
                        user {
                          login
                        }
                      }
                    }
                    totalCount
                  }
                }
              }
            }
            languages(first: 10) {
              nodes {
                name
              }
            }
            refs(refPrefix: "refs/tags/", first: 10) {
              totalCount
            }
            releases {
              totalCount
            }
            openIssues: issues(states: OPEN) {
              totalCount
            }
            closedIssues: issues(states: CLOSED) {
              totalCount
            }
            pushedAt
            updatedAt
            isArchived
          }
        }
      }
    }
    """
    repo_details = []
    cursor = None

    while True:
        variables = {"org": org_name, "cursor": cursor}
        try:
            response = requests.post(API_URL, json={"query": query, "variables": variables}, headers=get_headers(), timeout=10)
            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                logging.error(f"GraphQL errors for {org_name}: {data['errors']}")
                print(f"GraphQL errors for {org_name}: {data['errors']}")
                switch_token()
                continue

            repos = data.get("data", {}).get("organization", {}).get("repositories", {}).get("nodes", [])
            page_info = data.get("data", {}).get("organization", {}).get("repositories", {}).get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")

            if not repos:
                print(f"No repositories found for organization {org_name}.")
                break

            for repo in repos:
                if repo is None:
                    logging.error(f"Repository entry is None in organization {org_name}")
                    print(f"Repository entry is None in organization {org_name}")
                    continue

                try:
                    name = repo.get("name", "N/A")
                    is_private = "Private" if repo.get("isPrivate", False) else "Public"
                    disk_usage = repo.get("diskUsage", "N/A")
                    default_branch = repo.get("defaultBranchRef", {}).get("name", "N/A")
                    total_commits = repo.get("defaultBranchRef", {}).get("target", {}).get("history", {}).get("totalCount", 0)
                    last_pusher = repo.get("defaultBranchRef", {}).get("target", {}).get("history", {}).get("nodes", [{}])[0].get("author", {}).get("user", {}).get("login", "N/A")
                    open_pull_requests, closed_pull_requests = fetch_pull_requests_count(name, org_name)
                    open_issues = repo.get("openIssues", {}).get("totalCount", 0)
                    closed_issues = repo.get("closedIssues", {}).get("totalCount", 0)
                    languages = ", ".join([lang.get("name", "N/A") for lang in repo.get("languages", {}).get("nodes", [])])
                    total_releases = repo.get("releases", {}).get("totalCount", 0)
                    total_tags = repo.get("refs", {}).get("totalCount", 0)
                    last_pushed_at = repo.get("pushedAt", "N/A")
                    last_updated_at = repo.get("updatedAt", "N/A")
                    is_archived = "Yes" if repo.get("isArchived", False) else "No"

                    repo_info = {
                        "Repository Name": name,
                        "Visibility": is_private,
                        "Size (KB)": disk_usage,
                        "Default Branch": default_branch,
                        "Total Commits": total_commits,
                        "Last Pusher": last_pusher,
                        "Open Pull Requests": open_pull_requests,
                        "Closed Pull Requests": closed_pull_requests,
                        "Open Issues": open_issues,
                        "Closed Issues": closed_issues,
                        "Languages": languages,
                        "Total Releases": total_releases,
                        "Total Tags": total_tags,
                        "Last Pushed At": last_pushed_at,
                        "Last Updated At": last_updated_at,
                        "Archived": is_archived
                    }
                    repo_details.append(repo_info)

                except Exception as e:
                    logging.error(f"Error processing repository {repo.get('name', 'N/A')} in organization {org_name}: {e}")
                    print(f"Error processing repository {repo.get('name', 'N/A')} in organization {org_name}: {e}")

            if not has_next_page:
                break
            sleep(1)

        except requests.exceptions.RequestException as e:
            logging.error(f"Network error for organization {org_name}: {e}")
            print(f"Network error for organization {org_name}: {e}")
            switch_token()
        except Exception as e:
            logging.error(f"Unexpected error for organization {org_name}: {e}")
            print(f"Unexpected error for organization {org_name}: {e}")
            switch_token()

    return repo_details

def main():
    for org in ORG_NAMES:
        print(f"Processing organization: {org}")
        repos = fetch_org_repos(org)

        if repos:
            csv_filename = f"{org}_repo_details.csv"
            with open(csv_filename, "w", newline="") as csvfile:
                fieldnames = [
                    "Repository Name", "Visibility", "Size (KB)", "Default Branch", "Total Commits",
                    "Last Pusher", "Open Pull Requests", "Closed Pull Requests", "Open Issues", "Closed Issues",
                    "Languages", "Total Releases", "Total Tags", "Last Pushed At",
                    "Last Updated At", "Archived"
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(repos)
            
            print(f"Data for organization '{org}' written to {csv_filename}")
        else:
            print(f"No data fetched for organization '{org}'.")

if __name__ == "__main__":
    main()
