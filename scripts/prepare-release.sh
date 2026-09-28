#!/usr/bin/env bash
#
# Prepares the repository for a release tag.
#
# It promotes the [Unreleased] section of CHANGELOG.md into a numbered release,
# opens a fresh [Unreleased] section, rewrites the comparison links, syncs the
# supported version in SECURITY.md and writes the release notes to a file for
# "gh release create --notes-file".
#
# It deliberately does not commit, tag or push. Inspect "git diff" and do that
# yourself.
#
# Usage:
#   scripts/prepare-release.sh v0.12.0
#   scripts/prepare-release.sh v0.12.0 --date 2026-09-15
#   scripts/prepare-release.sh v0.12.0 --notes-file /tmp/notes.md
#   scripts/prepare-release.sh v0.12.0 --allow-dirty

set -euo pipefail

readonly CHANGELOG="CHANGELOG.md"
readonly SECURITY="SECURITY.md"
readonly UNRELEASED_HEADING="## [Unreleased]"
readonly DEFAULT_NOTES_FILE="release-notes.md"

die() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

info() {
	printf '%s\n' "$*"
}

usage() {
	sed -n '3,17p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
}

# parse_args reads the command line into the globals used by the rest of the
# script.
parse_args() {
	version=""
	release_date=""
	notes_file="$DEFAULT_NOTES_FILE"
	allow_dirty=0

	while [[ $# -gt 0 ]]; do
		case "$1" in
		-h | --help)
			usage
			;;
		--date)
			[[ $# -ge 2 ]] || die "--date needs a value"
			release_date="$2"
			shift 2
			;;
		--notes-file)
			[[ $# -ge 2 ]] || die "--notes-file needs a value"
			notes_file="$2"
			shift 2
			;;
		--allow-dirty)
			allow_dirty=1
			shift
			;;
		-*)
			die "unknown option: $1"
			;;
		*)
			[[ -z "$version" ]] || die "version given twice: $version and $1"
			version="$1"
			shift
			;;
		esac
	done

	[[ -n "$version" ]] || usage

	# The tag is vX.Y.Z; the changelog heading drops the leading v.
	[[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
		die "version must look like v0.12.0, got: $version"

	version_number="${version#v}"

	if [[ -z "$release_date" ]]; then
		release_date="$(date -u +%Y-%m-%d)"
	fi

	[[ "$release_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] ||
		die "date must be YYYY-MM-DD, got: $release_date"
}

# check_repo_state refuses to run anywhere the result would be confusing.
check_repo_state() {
	command -v git >/dev/null || die "git is not on PATH"

	local root
	root="$(git rev-parse --show-toplevel 2>/dev/null)" ||
		die "not inside a git repository"
	cd "$root"

	[[ -f "$CHANGELOG" ]] || die "$CHANGELOG not found in $root"
	[[ -f "$SECURITY" ]] || die "$SECURITY not found in $root"

	if git rev-parse -q --verify "refs/tags/$version" >/dev/null; then
		die "tag $version already exists"
	fi

	if grep -qF "## [$version_number]" "$CHANGELOG"; then
		die "$CHANGELOG already has a section for $version_number"
	fi

	if [[ "$allow_dirty" -eq 0 ]] && [[ -n "$(git status --porcelain)" ]]; then
		die "working tree is dirty; commit or stash first, or pass --allow-dirty"
	fi
}

# read_previous_version takes the previous tag from the [Unreleased] comparison
# link rather than from git, because the newest tag is not always the previous
# release: v0.10.1 was a backport published after v0.11.1.
read_previous_version() {
	local line
	line="$(grep -m1 '^\[Unreleased\]:' "$CHANGELOG")" ||
		die "$CHANGELOG has no [Unreleased] link at the bottom"

	previous_version="$(sed -E 's|.*/compare/(v[0-9]+\.[0-9]+\.[0-9]+)\.\.\.HEAD[[:space:]]*$|\1|' <<<"$line")"
	[[ "$previous_version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
		die "cannot read the previous version from: $line"

	# Everything before /compare/ is the repository URL, so the script does not
	# have to guess it from the git remote.
	repo_url="$(sed -E 's|^\[Unreleased\]:[[:space:]]*(.*)/compare/.*$|\1|' <<<"$line")"
	[[ "$repo_url" == http* ]] || die "cannot read the repository URL from: $line"
}

# check_unreleased_not_empty catches the common mistake of tagging a release
# nobody wrote an entry for.
check_unreleased_not_empty() {
	local body
	body="$(extract_section "$UNRELEASED_HEADING")"

	if [[ -z "${body//[[:space:]]/}" ]]; then
		die "the [Unreleased] section is empty; write the entries before releasing"
	fi
}

# extract_section prints the body of a section, without its heading and without
# the surrounding blank lines.
extract_section() {
	local heading="$1"

	awk -v heading="$heading" '
		$0 == heading { inside = 1; next }
		inside && /^## / { exit }
		inside { print }
	' "$CHANGELOG" | trim_blank_lines
}

# trim_blank_lines drops leading and trailing blank lines from stdin.
trim_blank_lines() {
	awk '
		NF {
			# Blank lines held back so far turned out to be interior ones.
			for (i = 0; i < pending; i++) print ""
			pending = 0
			started = 1
			print
			next
		}
		started { pending++ }
	'
}

# rewrite_changelog inserts the new release heading under [Unreleased] and adds
# the comparison links.
rewrite_changelog() {
	local tmp
	tmp="$(mktemp)"

	awk \
		-v unreleased="$UNRELEASED_HEADING" \
		-v heading="## [$version_number] - $release_date" \
		-v new_unreleased_link="[Unreleased]: $repo_url/compare/$version...HEAD" \
		-v new_release_link="[$version_number]: $repo_url/compare/$previous_version...$version" '
		$0 == unreleased && !promoted {
			# The blank line that already follows the heading separates the new
			# release heading from the entries, so nothing else is printed here.
			print unreleased
			print ""
			print heading
			promoted = 1
			next
		}
		/^\[Unreleased\]:/ && !linked {
			print new_unreleased_link
			print new_release_link
			linked = 1
			next
		}
		{ print }
		END {
			if (!promoted) { print "missing-unreleased-heading" > "/dev/stderr"; exit 1 }
			if (!linked) { print "missing-unreleased-link" > "/dev/stderr"; exit 1 }
		}
	' "$CHANGELOG" >"$tmp" || {
		rm -f "$tmp"
		die "failed to rewrite $CHANGELOG"
	}

	mv "$tmp" "$CHANGELOG"
	info "updated $CHANGELOG: $version_number - $release_date (previous: $previous_version)"
}

# sync_security_version keeps the supported version table pointing at the tag
# being released.
sync_security_version() {
	if ! grep -qE '`v[0-9]+\.[0-9]+\.[0-9]+` \(latest tag\)' "$SECURITY"; then
		info "warning: no supported version row found in $SECURITY, skipping it"
		return
	fi

	local tmp
	tmp="$(mktemp)"

	sed -E "s/\`v[0-9]+\.[0-9]+\.[0-9]+\` \(latest tag\)/\`$version\` (latest tag)/g" \
		"$SECURITY" >"$tmp" || {
		rm -f "$tmp"
		die "failed to rewrite $SECURITY"
	}

	mv "$tmp" "$SECURITY"
	info "updated $SECURITY: supported version is now $version"
}

# write_release_notes dumps the new section so it can be handed to gh.
write_release_notes() {
	extract_section "## [$version_number] - $release_date" >"$notes_file"

	[[ -s "$notes_file" ]] || die "the extracted release notes are empty"

	info "wrote release notes to $notes_file"
}

main() {
	parse_args "$@"
	check_repo_state
	read_previous_version
	check_unreleased_not_empty
	rewrite_changelog
	sync_security_version
	write_release_notes

	cat <<-EOF

		Done. Nothing was committed, tagged or pushed.

		Next:
		  git diff
		  # commit the changes and open a pull request against dev
		  # after it is merged and CI is green:
		  #   git tag -a $version -m "$version"
		  #   git push origin $version
		  #   gh release create $version --notes-file $notes_file
	EOF
}

main "$@"
