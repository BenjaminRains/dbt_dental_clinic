import React, { useEffect, useRef, useState } from 'react';
import { Box } from '@mui/material';

interface MermaidDiagramProps {
    chart: string;
    id?: string;
}

let renderQueue: Promise<void> = Promise.resolve();
let renderSeq = 0;

function enqueue<T>(task: () => Promise<T>): Promise<T> {
    const run = renderQueue.then(task, task);
    renderQueue = run.then(
        () => undefined,
        () => undefined,
    );
    return run;
}

const MermaidDiagram: React.FC<MermaidDiagramProps> = ({ chart, id = 'mermaid-diagram' }) => {
    const mermaidRef = useRef<HTMLDivElement>(null);
    const [isLoaded, setIsLoaded] = useState(false);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        let cancelled = false;
        const seq = ++renderSeq;
        const uniqueId = `mdc${id.replace(/[^A-Za-z0-9]/g, '')}${seq}`;

        enqueue(async () => {
            if (cancelled || !mermaidRef.current) return;

            // @ts-ignore - mermaid types may not be available at compile time
            const mermaidModule = await import('mermaid');
            const mermaid = mermaidModule.default || mermaidModule;

            mermaid.initialize({
                startOnLoad: false,
                securityLevel: 'strict',
                theme: 'default',
                flowchart: {
                    useMaxWidth: true,
                    htmlLabels: true,
                    curve: 'basis',
                    nodeSpacing: 40,
                    rankSpacing: 50,
                    padding: 16,
                },
                themeVariables: {
                    primaryColor: '#1976d2',
                    primaryTextColor: '#fff',
                    primaryBorderColor: '#1565c0',
                    lineColor: '#666',
                    secondaryColor: '#e3f2fd',
                    tertiaryColor: '#f5f5f5',
                    fontSize: '16px',
                },
            });

            const { svg } = await mermaid.render(uniqueId, chart.trim());
            if (cancelled || !mermaidRef.current) return;
            mermaidRef.current.innerHTML = svg;
            setIsLoaded(true);
            setError(null);
        }).catch((renderError) => {
            if (cancelled) return;
            console.error('Mermaid rendering error:', renderError);
            if (mermaidRef.current) {
                mermaidRef.current.innerHTML = '';
            }
            setError('Failed to render diagram');
            setIsLoaded(false);
        });

        return () => {
            cancelled = true;
        };
    }, [chart, id]);

    return (
        <Box sx={{ width: '100%' }}>
            {error ? (
                <Box sx={{ color: 'error.main', textAlign: 'center', py: 2 }}>{error}</Box>
            ) : null}
            <Box
                ref={mermaidRef}
                sx={{
                    display: error ? 'none' : 'flex',
                    justifyContent: 'center',
                    alignItems: 'center',
                    width: '100%',
                    overflowX: 'auto',
                    minHeight: isLoaded || error ? 'auto' : 160,
                    '& svg': {
                        width: '100% !important',
                        maxWidth: '100%',
                        height: 'auto',
                    },
                }}
            />
        </Box>
    );
};

export default MermaidDiagram;
