import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export const frontendRoot = path.dirname(fileURLToPath(import.meta.url))

export function createAppViteConfig(appRoot: string) {
    return defineConfig(({ command }) => {
        const isDev = command === 'serve'
        const isProduction = command === 'build'

        return {
            // Config lives in apps/<name>; keep root explicit for clarity.
            root: appRoot,
            envDir: appRoot,
            plugins: [react()],
            resolve: {
                alias: [
                    {
                        find: '@mdc/analytics-ui/pages',
                        replacement: path.resolve(frontendRoot, 'packages/analytics-ui/src/pages'),
                    },
                    {
                        find: '@mdc/analytics-api/clinic',
                        replacement: path.resolve(frontendRoot, 'packages/analytics-api/src/authApi.ts'),
                    },
                    {
                        find: '@mdc/analytics-api',
                        replacement: path.resolve(frontendRoot, 'packages/analytics-api/src/index.ts'),
                    },
                    {
                        find: '@mdc/analytics-ui',
                        replacement: path.resolve(frontendRoot, 'packages/analytics-ui/src/index.ts'),
                    },
                    {
                        find: '@mdc/ui-common',
                        replacement: path.resolve(frontendRoot, 'packages/ui-common/src/index.ts'),
                    },
                ],
            },
            optimizeDeps: {
                include: [
                    'react',
                    'react-dom',
                    'react/jsx-runtime',
                    'react-router-dom',
                    'scheduler',
                ],
            },
            ...(isDev && {
                server: {
                    port: 3000,
                    proxy: {
                        '/api': {
                            // Override when port 8000 is occupied by a stale API:
                            //   $env:MDC_API_PROXY_TARGET='http://127.0.0.1:8001'
                            target:
                                process.env.MDC_API_PROXY_TARGET ||
                                'http://localhost:8000',
                            changeOrigin: true,
                            rewrite: (p: string) => p.replace(/^\/api/, ''),
                        },
                    },
                },
            }),
            ...(isProduction && {
                build: {
                    outDir: 'dist',
                    emptyOutDir: true,
                    sourcemap: false,
                    rollupOptions: {
                        output: {
                            entryFileNames: 'assets/[name]-[hash].js',
                            chunkFileNames: 'assets/[name]-[hash].js',
                            manualChunks: (id: string) => {
                                if (id.includes('node_modules')) {
                                    if (
                                        id.includes('/react/') &&
                                        !id.includes('react-dom') &&
                                        !id.includes('react-router') &&
                                        !id.includes('react/jsx-runtime')
                                    ) {
                                        return undefined
                                    }
                                    if (id.includes('scheduler')) {
                                        return undefined
                                    }
                                    if (id.includes('axios') || id.includes('zustand')) {
                                        return 'utils-vendor'
                                    }
                                    if (id.includes('mermaid')) {
                                        return 'mermaid-vendor'
                                    }
                                    return 'react-vendor'
                                }
                            },
                        },
                    },
                    chunkSizeWarningLimit: 500,
                },
            }),
        }
    })
}
