import React, { useEffect, useRef, useState } from 'react';
import { Box } from '@mui/material';

interface MermaidDiagramProps {
    chart: string;
    id?: string;
}

const MermaidDiagram: React.FC<MermaidDiagramProps> = ({ chart, id = 'mermaid-diagram' }) => {
    const mermaidRef = useRef<HTMLDivElement>(null);
    const [isLoaded, setIsLoaded] = useState(false);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        const loadMermaid = async () => {
            if (!mermaidRef.current) return;

            try {
                // @ts-ignore - mermaid types may not be available at compile time
                const mermaidModule = await import('mermaid');
                const mermaid = mermaidModule.default || mermaidModule;

                // Initialize Mermaid
                mermaid.initialize({
                    startOnLoad: false,
                    theme: 'default',
                    flowchart: {
                        useMaxWidth: true,
                        htmlLabels: true,
                        curve: 'basis',
                    },
                    themeVariables: {
                        primaryColor: '#1976d2',
                        primaryTextColor: '#fff',
                        primaryBorderColor: '#1565c0',
                        lineColor: '#666',
                        secondaryColor: '#e3f2fd',
                        tertiaryColor: '#f5f5f5',
                    },
                });

                // Clear previous content
                mermaidRef.current.innerHTML = '';

                // Create a unique ID for this diagram
                const uniqueId = `${id}-${Date.now()}`;

                // Create a new element with the mermaid class
                const diagramElement = document.createElement('div');
                diagramElement.className = 'mermaid';
                diagramElement.id = uniqueId;
                diagramElement.textContent = chart.trim();

                mermaidRef.current.appendChild(diagramElement);

                // Render the diagram
                try {
                    await mermaid.run({
                        nodes: [diagramElement],
                    });
                    setIsLoaded(true);
                    setError(null);
                } catch (renderError) {
                    console.error('Mermaid rendering error:', renderError);
                    setError('Failed to render diagram');
                    setIsLoaded(false);
                }
            } catch (error) {
                console.error('Failed to load mermaid:', error);
                setError('Failed to load Mermaid library');
                setIsLoaded(false);
            }
        };

        loadMermaid();
    }, [chart, id]);

    if (error) {
        return (
            <Box
                sx={{
                    p: 2,
                    textAlign: 'center',
                    color: 'error.main',
                }}
            >
                {error}
            </Box>
        );
    }

    return (
        <Box
            ref={mermaidRef}
            sx={{
                display: 'flex',
                justifyContent: 'center',
                alignItems: 'center',
                width: '100%',
                overflowX: 'auto',
                minHeight: isLoaded ? 'auto' : '400px',
                '& .mermaid': {
                    display: 'flex',
                    justifyContent: 'center',
                },
                '& svg': {
                    maxWidth: '100%',
                    height: 'auto',
                },
            }}
        />
    );
};

export default MermaidDiagram;

