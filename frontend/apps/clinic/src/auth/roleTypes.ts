export type UserRole = 'practice-manager' | 'owner' | 'front-desk' | 'insurance';

export const ROLE_LABELS: Record<UserRole, string> = {
    'practice-manager': 'Practice Manager',
    owner: 'Owner',
    'front-desk': 'Front Desk',
    insurance: 'Insurance Specialist',
};

export const ROLE_HOME_PATHS: Record<UserRole, string> = {
    'practice-manager': '/home/practice-manager',
    owner: '/home/owner',
    'front-desk': '/home/front-desk',
    insurance: '/home/insurance',
};
