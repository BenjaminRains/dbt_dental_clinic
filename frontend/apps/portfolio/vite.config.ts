import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { createAppViteConfig } from '../../vite.shared'

const appRoot = path.dirname(fileURLToPath(import.meta.url))

export default createAppViteConfig(appRoot)
