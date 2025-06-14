# Integrated Source File Refactoring Prompting System

## How the Two Documents Work Together

### **Document 1: Strategic Foundation** (`source_file_standardization_strategy.md`)
**Purpose**: Provides comprehensive context, patterns, and implementation framework
**Role**: Strategic guidance and detailed reference material

### **Document 2: AI Analyst Instructions** (`source_file_refactor_template.md`) 
**Purpose**: Actionable prompt template for AI analysts to execute refactoring
**Role**: Tactical implementation guide and quality standards

## **Integrated Prompting Workflow**

### **Step 1: Provide Strategic Context** 
Use `source_file_standardization_strategy.md` to give AI analysts:
- Understanding of the overall refactoring strategy
- Context about system classification (A-G framework)
- Examples of excellent vs. poor documentation
- Technical patterns and business context requirements

### **Step 2: Execute with Tactical Template**
Use `source_file_refactor_template.md` as the direct prompt with:
- Specific formatting requirements
- Quality checklists
- Column-by-column documentation patterns
- Deliverable specifications

## **Complete Prompting System**

### **For AI Analysts - Full Prompt Structure:**

```
CONTEXT (from source_file_standardization_strategy.md):
You are refactoring OpenDental source files for MDC Analytics. This organization has:
- Excellent reference files: billing_sources.yml and claims_sources.yml
- System classification framework (A-G) for business organization
- Established naming conventions and staging model standards
- Quality tiers ranging from excellent to minimal documentation

OBJECTIVE:
Transform the attached source file to match the quality of the reference files.

REFERENCE EXAMPLES:
[Include excerpts from billing_sources.yml and claims_sources.yml showing excellent patterns]

INSTRUCTIONS (from source_file_refactor_template.md):
[Full template with specific patterns, requirements, and quality gates]

SOURCE FILE TO REFACTOR:
[Attach the specific .yml file to be enhanced]

DELIVERABLE:
Enhanced source file following all template requirements and quality standards.
```

## **Specific Usage Scenarios**

### **Scenario 1: Enhancing Good Foundation Files**
**Example**: `appointment_sources.yml` (good descriptions, missing meta sections)

**Prompt Construction:**
```
CONTEXT: 
Use source_file_standardization_strategy.md to understand that this file has "good foundation but needs enhancement" - specifically missing comprehensive meta sections and business context.

REFERENCE:
Look at billing_sources.yml meta sections as the target quality level.

TASK:
Using source_file_refactor_template.md patterns, add comprehensive meta sections while preserving existing good column descriptions.

FOCUS AREAS:
- Add complete meta sections with data quality assessments
- Enhance business context for System E: Scheduling & Resources
- Standardize test patterns with appropriate severity levels
```

### **Scenario 2: Transforming Minimal Documentation**
**Example**: `coding_defs_sources.yml` (basic structure, needs significant work)

**Prompt Construction:**
```
CONTEXT:
Use source_file_standardization_strategy.md to understand this file needs "major enhancement" - minimal business context and missing comprehensive documentation.

REFERENCE:
Study both reference files to understand the level of business context and technical detail required.

TASK:
Using source_file_refactor_template.md, completely transform this file from minimal to comprehensive documentation.

FOCUS AREAS:
- Add extensive business context for System Support: Configuration & Reference
- Document all definition categories with business meanings
- Create comprehensive meta sections from scratch
- Add appropriate test coverage for configuration data
```

### **Scenario 3: Creating New Consolidated Files**
**Example**: Creating unified provider/user management sources

**Prompt Construction:**
```
CONTEXT:
Use source_file_standardization_strategy.md to understand the need for consolidated files and system organization principles.

REFERENCE:
Follow the comprehensive patterns from billing_sources.yml and claims_sources.yml.

TASK:
Using source_file_refactor_template.md, create a new consolidated source file that combines provider and user management tables.

FOCUS AREAS:
- Classify as System D: Patient & Provider Management
- Document workforce management workflows
- Create unified governance and ownership sections
- Establish consistent relationship documentation across tables
```

## **Quality Assurance Integration**

### **Two-Layer Quality Control:**

**Layer 1: Strategic Alignment**
Using `source_file_standardization_strategy.md`:
- Does it match the system classification framework?
- Is the business context comprehensive like the reference files?
- Does it support the staging model development goals?

**Layer 2: Tactical Compliance**
Using `source_file_refactor_template.md`:
- Does it follow the exact template structure?
- Are all required meta sections present and complete?
- Do column patterns match the specified formats?
- Are quality checklists satisfied?

## **Implementation Recommendations**

### **For Data Engineering Team:**

**1. Initial AI Analyst Briefing:**
- Provide both documents as package
- Walk through 1-2 examples using both documents
- Establish review process using both quality layers

**2. Per-File Prompting:**
```bash
# Provide strategic context
cat source_file_standardization_strategy.md

# Provide tactical template
cat source_file_refactor_template.md

# Specify current file and enhancement level needed
echo "CURRENT FILE: appointment_sources.yml"
echo "ENHANCEMENT LEVEL: Good foundation, needs meta sections"
echo "SYSTEM CLASSIFICATION: System E - Scheduling & Resources"

# Attach file to be refactored
cat appointment_sources.yml
```

**3. Review Process:**
- **Strategic Review**: Does it align with overall strategy and reference quality?
- **Tactical Review**: Does it meet all template requirements and checklists?
- **Integration Review**: Will it support staging model development effectively?

### **For AI Analysts:**

**1. Read Strategy Document First:**
- Understand overall goals and quality expectations
- Study reference files to internalize quality standards
- Learn system classification and business context requirements

**2. Use Template as Working Guide:**
- Follow column patterns exactly
- Complete all required meta sections
- Apply appropriate test patterns
- Validate against quality checklists

**3. Iterative Improvement:**
- Compare output to reference files
- Ensure business context meets strategic requirements
- Validate technical compliance with template standards

## **Expected Workflow**

### **Phase 1: Understanding** (use strategy document)
AI analyst reviews overall approach, reference files, and strategic context

### **Phase 2: Execution** (use template document)
AI analyst applies specific patterns and requirements to transform the source file

### **Phase 3: Validation** (use both documents)
Review output against both strategic goals and tactical requirements

This integrated approach ensures that AI analysts understand both the "why" (strategic context) and the "how" (tactical execution) for creating consistently excellent source file documentation that supports your broader data architecture goals.