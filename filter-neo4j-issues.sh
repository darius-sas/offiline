#!/usr/bin/env bash
set -euo pipefail

jaq '
def one:
  if . == null then null
  elif type == "array" then .[0] // null
  elif type == "object" then
    if length == 0 then null
    else .[keys_unsorted[0]]
    end
  else .
  end;

[
  .issues
  | to_entries[]
  | .key as $issue_id
  | .value as $parent
  | ($parent.versionedRepresentations // {}) as $v
  | {
      issue_id: $issue_id,
      name: $parent.key,
      summary: (($v.summary // null) | one),
      description: (($v.description // null) | one),
      creation_date: (($v.created // null) | one),
      itype: ((($v.issuetype // null) | one)?.name // null),
      priority: ((($v.priority // null) | one)?.name // null),
      status: (($v.status.name // null | one) // null),
      status_category: ((($v.status // null) | one)?.statusCategory.name // null),
      closed_date: (($v.resolutiondate // null) | one),
      update_date: (($v.updated // null) | one),
      scope: ((($v.project // null) | one)?.name // null),
      project_category: ((($v.project // null) | one)?.projectCategory.name // null),
      time_estimate: (($v.aggregatetimeestimate // null) | one),
      time_spent: (($parent.renderedFields.aggregatetimespent // null) | one), 
      parent: (($parent.fields.parent // null)?.key // null),
      epic: (($v.customfield_10200 // null | one) // null),
      story_points: (($v.customfield_10002 // null | one) // null),
      original_story_points: (($v.customfield_11034 // null | one) // null),
      account_name: (($v.customfield_10600 // null | one)?.name // null),
      account_key: (($v.customfield_10600 // null | one)?.key // null),
      account_id: (($v.customfield_10600 // null | one)?.id // null)
    }
]
' issues/issues-dump.json > issues/filtered-issues.json
