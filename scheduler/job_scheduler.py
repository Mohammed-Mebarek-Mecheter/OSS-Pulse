# job_scheduler.py
import os
import logging
from pocketbase.client import ClientResponseError
from pocketbase import PocketBase
from scheduler.apscheduler_config import create_scheduler
from data_collection.data_inserter import insert_data
from data_processing.cleaner import clean_all_data
from data_processing.transformer import transform_all_data
from datetime import datetime
from dotenv import load_dotenv
import requests

# Load environment variables
load_dotenv()

# PocketBase configuration
POCKETBASE_URL = os.getenv("POCKETBASE_URL")
POCKETBASE_EMAIL = os.getenv("POCKETBASE_EMAIL")
POCKETBASE_PASSWORD = os.getenv("POCKETBASE_PASSWORD")

# Initialize PocketBase client
pb = PocketBase(POCKETBASE_URL)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

# Check if environment variables are loaded
required_vars = ['GITHUB_TOKEN', 'POCKETBASE_URL', 'POCKETBASE_EMAIL', 'POCKETBASE_PASSWORD']
for var in required_vars:
    if not os.getenv(var):
        raise EnvironmentError(f"{var} is not set in the environment or .env file")

def authenticate_pocketbase():
    try:
        # Authenticate as an admin user
        admin_admin = pb.admins.auth_with_password(POCKETBASE_EMAIL, POCKETBASE_PASSWORD)
        logging.info("Successfully authenticated with PocketBase")
    except ClientResponseError as e:
        logging.error(f"Failed to authenticate with PocketBase: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during PocketBase authentication: {e}")
        raise

def scheduled_data_collection_and_processing(owner, repo):
    """
    The job function that will be scheduled to run at regular intervals.
    This function will collect data from GitHub, insert it into PocketBase,
    and then clean and transform the data for further use.
    """
    try:
        logging.info(f"Starting data collection for repository: {owner}/{repo}")
        authenticate_pocketbase()
        logging.info(f"Calling insert_data for {owner}/{repo}")
        insert_data(owner, repo)
        logging.info(f"Data collection completed for repository: {owner}/{repo}")

        # After data insertion, clean and transform the data
        logging.info(f"Starting data processing for repository: {owner}/{repo}")
        repo_clean, issues_clean, pr_clean = clean_all_data()
        repo_transformed, issues_transformed, pr_transformed = transform_all_data(repo_clean, issues_clean, pr_clean)
        logging.info(f"Data processing completed for repository: {owner}/{repo}")

    except Exception as e:
        logging.error(f"Error during data collection and processing for {owner}/{repo}: {e}")
        logging.exception("Traceback:")

def start_scheduler():
    """
    Starts the APScheduler with the defined jobs.
    """
    scheduler = create_scheduler()

    # Define the repositories you want to schedule data collection for
    repositories = [
        {'owner': 'mozilla', 'repo': 'firefox'},
        {'owner': 'torvalds', 'repo': 'linux'},
        {'owner': 'apple', 'repo': 'swift'},
        {'owner': 'google', 'repo': 'guava'},
        {'owner': 'apache', 'repo': 'httpd'},
        {'owner': 'git', 'repo': 'git'},
        {'owner': 'nodejs', 'repo': 'node'},
        {'owner': 'npm', 'repo': 'cli'},
        {'owner': 'yarnpkg', 'repo': 'yarn'},
        {'owner': 'webpack', 'repo': 'webpack'},
        {'owner': 'babel', 'repo': 'babel'},
        {'owner': 'rollup', 'repo': 'rollup'},
        {'owner': 'parcel-bundler', 'repo': 'parcel'},
        {'owner': 'vitejs', 'repo': 'vite'},
        {'owner': 'facebook', 'repo': 'jest'},
        {'owner': 'mochajs', 'repo': 'mocha'},
        {'owner': 'avajs', 'repo': 'ava'},
        {'owner': 'cypress-io', 'repo': 'cypress'},
        {'owner': 'puppeteer', 'repo': 'puppeteer'},
        {'owner': 'microsoft', 'repo': 'vscode'},
        {'owner': 'atom', 'repo': 'atom'},
        {'owner': 'sublimehq', 'repo': 'sublime_text'},
        {'owner': 'emacs-mirror', 'repo': 'emacs'},
        {'owner': 'vim', 'repo': 'vim'},
        {'owner': 'neovim', 'repo': 'neovim'},
        {'owner': 'ohmyzsh', 'repo': 'ohmyzsh'},
        {'owner': 'tmux', 'repo': 'tmux'},
        {'owner': 'fish-shell', 'repo': 'fish-shell'},
        {'owner': 'zsh-users', 'repo': 'zsh'},
        {'owner': 'gnome', 'repo': 'gnome-shell'},
        {'owner': 'kde', 'repo': 'plasma-desktop'},
        {'owner': 'lxde', 'repo': 'lxde'},
        {'owner': 'xfce', 'repo': 'xfce4'},
        {'owner': 'mate-desktop', 'repo': 'mate-desktop'},
        {'owner': 'cinnamon', 'repo': 'cinnamon'},
        {'owner': 'elementary', 'repo': 'elementary'},
        {'owner': 'deepin-community', 'repo': 'deepin-desktop'},
        {'owner': 'solus-project', 'repo': 'budgie-desktop'},
        {'owner': 'pop-os', 'repo': 'pop'},
        {'owner': 'linuxmint', 'repo': 'linuxmint'},
        {'owner': 'ubuntu', 'repo': 'ubuntu'},
        {'owner': 'debian', 'repo': 'debian'},
        {'owner': 'archlinux', 'repo': 'archiso'},
        {'owner': 'fedora', 'repo': 'fedora'},
        {'owner': 'opensuse', 'repo': 'openSUSE'},
        {'owner': 'manjaro', 'repo': 'manjaro'},
        {'owner': 'gentoo', 'repo': 'gentoo'},
        {'owner': 'void-linux', 'repo': 'void-packages'},
        {'owner': 'alpinelinux', 'repo': 'aports'},
        {'owner': 'nixos', 'repo': 'nixpkgs'},
        {'owner': 'clearlinux', 'repo': 'clear-linux-documentation'},
        {'owner': 'solus-project', 'repo': 'solus'},
        {'owner': 'freebsd', 'repo': 'freebsd'},
        {'owner': 'openbsd', 'repo': 'src'},
        {'owner': 'netbsd', 'repo': 'src'},
        {'owner': 'dragonflybsd', 'repo': 'dragonflybsd'},
        {'owner': 'haiku', 'repo': 'haiku'},
        {'owner': 'reactos', 'repo': 'reactos'},
        {'owner': 'serenityos', 'repo': 'serenity'},
        {'owner': 'redox-os', 'repo': 'redox'},
        {'owner': 'minix3', 'repo': 'minix'},
        {'owner': 'plan9', 'repo': 'plan9'},
        {'owner': 'illumos', 'repo': 'illumos-gate'},
        {'owner': 'smartos', 'repo': 'smartos-live'},
        {'owner': 'joyent', 'repo': 'node-triton'},
        {'owner': 'openindiana', 'repo': 'oi-userland'},
        {'owner': 'omniosorg', 'repo': 'omnios-build'},
        {'owner': 'openzfs', 'repo': 'zfs'},
        {'owner': 'zfsonlinux', 'repo': 'zfs'},
        {'owner': 'opencontainers', 'repo': 'runc'},
        {'owner': 'containers', 'repo': 'podman'},
        {'owner': 'docker', 'repo': 'docker'},
        {'owner': 'moby', 'repo': 'moby'},
        {'owner': 'containerd', 'repo': 'containerd'},
        {'owner': 'openstack', 'repo': 'openstack'},
        {'owner': 'ceph', 'repo': 'ceph'},
        {'owner': 'gluster', 'repo': 'glusterfs'},
        {'owner': 'rook', 'repo': 'rook'},
        {'owner': 'minio', 'repo': 'minio'},
        {'owner': 'etcd-io', 'repo': 'etcd'},
        {'owner': 'coreos', 'repo': 'etcd'},
        {'owner': 'prometheus', 'repo': 'alertmanager'}
    ]

    # Schedule the job for each repository
    for repo in repositories:
        owner = repo['owner']
        repository = repo['repo']
        scheduler.add_job(
            scheduled_data_collection_and_processing,
            'interval',  # Schedule to run at intervals
            minutes=10,  # Adjust this interval as needed
            args=[owner, repository],
            id=f"{owner}_{repository}_job",  # Unique job ID
            next_run_time=datetime.now()  # Start immediately
        )

    # Start the scheduler
    scheduler.start()
    logging.info("Scheduler started. Jobs have been scheduled.")

    # Keep the script running
    try:
        while True:
            pass  # Replace with more sophisticated logic if needed
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logging.info("Scheduler shut down.")


if __name__ == "__main__":
    start_scheduler()