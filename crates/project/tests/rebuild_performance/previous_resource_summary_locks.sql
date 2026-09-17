            WHERE NOT EXISTS (
                SELECT 1 FROM v2_admin_powers admins
                WHERE admins.resource_id IN (resource.resource_id, root_resource.resource_id)
                  AND role.admin = ANY(admins.admins)
            )
