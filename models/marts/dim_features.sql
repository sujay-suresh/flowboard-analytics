{{
    config(
        materialized='table'
    )
}}

select
    feature_name,
    feature_category,
    min_plan_required

from (
    values
        ('kanban_board',    'core',           'free'),
        ('gantt_chart',     'project_mgmt',   'pro'),
        ('time_tracking',   'productivity',   'free'),
        ('file_storage',    'collaboration',  'free'),
        ('team_chat',       'collaboration',  'free'),
        ('automations',     'workflow',       'pro'),
        ('custom_fields',   'customization',  'pro'),
        ('reporting',       'analytics',      'pro'),
        ('api_access',      'integrations',   'enterprise'),
        ('sso_login',       'security',       'enterprise'),
        ('guest_access',    'collaboration',  'enterprise'),
        ('templates',       'productivity',   'free'),
        ('calendar_view',   'project_mgmt',   'free'),
        ('dependencies',    'project_mgmt',   'free'),
        ('workload_view',   'project_mgmt',   'pro')
) as features(feature_name, feature_category, min_plan_required)
