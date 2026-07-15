export type UserRole =
    | 'admin'
    | 'practice-manager'
    | 'owner'
    | 'front-desk'
    | 'insurance';

export const ALL_USER_ROLES: readonly UserRole[] = [
    'admin',
    'practice-manager',
    'owner',
    'front-desk',
    'insurance',
] as const;

export const ROLE_LABELS: Record<UserRole, string> = {
    admin: 'Admin',
    'practice-manager': 'Practice Manager',
    owner: 'Owner',
    'front-desk': 'Front Desk',
    insurance: 'Insurance Specialist',
};

export const ROLE_HOME_PATHS: Record<UserRole, string> = {
    admin: '/home/admin',
    'practice-manager': '/home/practice-manager',
    owner: '/home/owner',
    'front-desk': '/home/front-desk',
    insurance: '/home/insurance',
};

export function isUserRole(value: string): value is UserRole {
    return (ALL_USER_ROLES as readonly string[]).includes(value);
}
