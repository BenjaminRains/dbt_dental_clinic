import { useCallback, useEffect, useState } from 'react';
import { ApiResponse } from '../types/api';

type Fetcher<T> = () => Promise<ApiResponse<T>>;

export function useApiQuery<T>(
    fetcher: Fetcher<T>,
    deps: unknown[] = []
): ApiResponse<T> & { refetch: () => void } {
    const [state, setState] = useState<ApiResponse<T>>({ loading: true });

    const refetch = useCallback(() => {
        let cancelled = false;
        setState({ loading: true });

        fetcher()
            .then((result) => {
                if (!cancelled) {
                    setState(result);
                }
            })
            .catch(() => {
                if (!cancelled) {
                    setState({ loading: false, error: 'Failed to load data' });
                }
            });

        return () => {
            cancelled = true;
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, deps);

    useEffect(() => {
        const cleanup = refetch();
        return cleanup;
    }, [refetch]);

    return { ...state, refetch };
}
