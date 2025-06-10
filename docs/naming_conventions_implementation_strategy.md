# Naming Conventions Implementation Strategy
## MDC Analytics - ETL Pipeline & dbt Migration Plan

**Document Version:** 1.0  
**Created:** June 9th, 2025  
**Status:** Planning Phase  

---

## Executive Summary

This document outlines the strategic implementation plan for migrating from our current dual-convention 
naming system to the improved, simplified naming conventions documented in `Improved_naming_conventions.md`. 
The implementation will follow a phased approach to minimize risk and ensure system stability.

**Key Decision:** Complete ETL pipeline baseline first, then implement improved conventions for all new development.

---

## Current State Analysis

### Existing Convention Strengths
- ✅ **Clear distinction** between raw database columns (CamelCase) and derived fields (snake_case)
- ✅ **Working ETL pipeline** with 3.6M+ row extraction capability
- ✅ **Solid type conversion strategies** for PostgreSQL compatibility
- ✅ **Comprehensive metadata approach** with proper tracking

### Areas for Improvement
- ⚠️ **Multiple naming patterns** create cognitive overhead (CamelCase CTEs vs snake_case variables)
- ⚠️ **dbt community misalignment** with CamelCase CTE convention
- ⚠️ **Tool compatibility issues** with SQL formatters and IDEs
- ⚠️ **Complex environment variable naming** could be clearer

---

## Strategic Implementation Plan

### Phase 0: Current ETL Pipeline Completion ⭐ **ACTIVE**
**Timeline:** Immediate priority  
**Objective:** Establish stable, working baseline

**Goals:**
- ✅ Complete `securitylog` table ETL pipeline (3.6M+ rows)
- ✅ Verify extraction → replication → analytics flow
- ✅ Resolve PostgreSQL schema issues (`updated_at` column)
- ✅ Validate data quality and verification logic
- ✅ Document current ETL performance benchmarks

**Success Criteria:**
- [ ] `securitylog` data successfully in PostgreSQL analytics database
- [ ] End-to-end pipeline runs without errors
- [ ] Verification logic handles live database changes appropriately
- [ ] Load performance meets SLA requirements (< 20 minutes for 3.6M rows)

**Risk Mitigation:**
- Focus on stability over optimization
- Document all current configurations and conventions
- Create rollback procedures for any changes

---

### Phase 1: Foundation & Standards (Q1 2025)
**Timeline:** After ETL baseline established  
**Objective:** Create improved convention foundation

#### 1.1 Documentation Updates
**Tasks:**
- [ ] Finalize `Improved_naming_conventions.md` based on ETL learnings
- [ ] Create migration playbook with step-by-step procedures
- [ ] Document current system performance as baseline
- [ ] Create convention comparison guide (old vs new)

#### 1.2 Helper Infrastructure
**Tasks:**
- [ ] Create dbt macros for common patterns:
  ```sql
  {% macro convert_opendental_boolean(column_name) %}
  {% macro standardize_metadata_columns() %}
  {% macro transform_id_columns(source_column, target_name) %}
  ```
- [ ] Create environment variable template files
- [ ] Set up schema organization (staging, intermediate, marts)
- [ ] Create automated testing framework for conventions

#### 1.3 New Model Standards
**Tasks:**
- [ ] All new dbt models follow improved conventions
- [ ] Implement snake_case for all new CTEs
- [ ] Use standardized metadata approach
- [ ] Follow new schema organization patterns

**Success Criteria:**
- [ ] Helper macros tested and documented
- [ ] New model template established
- [ ] Zero new models using old conventions
- [ ] Team training completed on new standards

---

### Phase 2: Selective High-Impact Migration (Q2 2025)
**Timeline:** 3-6 months after baseline  
**Objective:** Migrate most valuable models first

#### 2.1 Priority Model Identification
**Criteria for Priority:**
- Frequently referenced staging models
- Models with complex CTE structures
- Models used by multiple downstream dependencies
- Models with poor readability due to naming

**Target Models:**
- `stg_opendental__patient` (high dependency)
- `stg_opendental__appointment` (complex CTEs)
- `stg_opendental__treatment` (frequently queried)
- Key fact and dimension tables

#### 2.2 Migration Process
**Steps per model:**
1. **Create parallel model** with new conventions
2. **Validate identical output** using data diff tools
3. **Update downstream dependencies** incrementally
4. **Deprecate old model** after 30-day transition
5. **Remove old model** after validation period

#### 2.3 Environment Variable Updates
**Tasks:**
- [ ] Update to descriptive naming (`OPENDENTAL_SOURCE_*`)
- [ ] Coordinate with deployment pipeline updates
- [ ] Update documentation and configuration management
- [ ] Test all connection functions with new variables

**Success Criteria:**
- [ ] Top 10 high-impact models migrated successfully
- [ ] Zero data discrepancies between old and new models
- [ ] Improved query readability demonstrated
- [ ] Team adoption of new patterns >80%

---

### Phase 3: Complete ETL Pipeline Migration (Q3 2025)
**Timeline:** After dbt model migration proven  
**Objective:** Update ETL pipeline to improved conventions

#### 3.1 Python Code Refactoring
**Tasks:**
- [ ] Update connection factory function names
- [ ] Refactor class and variable names for clarity
- [ ] Update tracking table names and schemas
- [ ] Implement new metadata strategy consistently

#### 3.2 Configuration Updates
**Tasks:**
- [ ] Update all environment variable references
- [ ] Refactor database connection logic
- [ ] Update logging and monitoring to new conventions
- [ ] Update CLI command naming and help text

#### 3.3 Performance Validation
**Tasks:**
- [ ] Benchmark performance vs. baseline
- [ ] Validate extraction/load times unchanged
- [ ] Test edge cases and error handling
- [ ] Update monitoring dashboards

**Success Criteria:**
- [ ] ETL pipeline performance maintained or improved
- [ ] Zero functional regressions
- [ ] All tracking and logging uses new conventions
- [ ] Documentation fully updated

---

### Phase 4: Ecosystem Completion (Q4 2025)
**Timeline:** Final implementation phase  
**Objective:** Complete migration and establish ongoing standards

#### 4.1 Final Migration Tasks
**Tasks:**
- [ ] Migrate remaining low-priority models
- [ ] Update all documentation to new conventions
- [ ] Remove all deprecated/old convention code
- [ ] Create style guide enforcement tools

#### 4.2 Team & Process Updates
**Tasks:**
- [ ] Final team training on complete system
- [ ] Update code review guidelines
- [ ] Create onboarding materials for new team members
- [ ] Establish convention monitoring/compliance

#### 4.3 Long-term Maintenance
**Tasks:**
- [ ] Set up automated convention checking
- [ ] Create quarterly convention review process
- [ ] Document lessons learned
- [ ] Plan for future convention evolution

**Success Criteria:**
- [ ] 100% of codebase follows new conventions
- [ ] Team confidence and adoption at 100%
- [ ] Documentation completely updated
- [ ] Maintenance processes established

---

## Risk Management

### High-Risk Areas
1. **Data Pipeline Downtime** during ETL migration
2. **Data Quality Issues** during model transitions
3. **Team Confusion** during convention changes
4. **Deployment Failures** due to environment variable changes

### Mitigation Strategies
1. **Parallel Development:** Run old and new systems simultaneously during transitions
2. **Comprehensive Testing:** Automated data validation between old and new models
3. **Clear Communication:** Regular team updates and training sessions
4. **Rollback Procedures:** Documented procedures for reverting changes
5. **Gradual Rollout:** Staged implementation with validation gates

---

## Success Metrics

### Technical Metrics
- **Performance:** Maintain or improve current ETL processing times
- **Quality:** Zero data discrepancies during migration
- **Reliability:** Maintain current uptime and error rates
- **Coverage:** 100% convention compliance in new code

### Team Metrics
- **Adoption Rate:** >90% team comfort with new conventions
- **Code Quality:** Improved readability scores in code reviews
- **Development Speed:** Maintained or improved development velocity
- **Knowledge Transfer:** Reduced onboarding time for new team members

### Business Metrics
- **System Stability:** No business impact during migration
- **Feature Delivery:** Maintained delivery schedule
- **Data Accuracy:** Continued high data quality scores
- **User Satisfaction:** No degradation in analytics user experience

---

## Communication Plan

### Stakeholder Updates
- **Weekly:** Technical team progress updates
- **Bi-weekly:** Management summary reports
- **Monthly:** Stakeholder impact assessments
- **Quarterly:** Strategic review and adjustment

### Documentation Updates
- **Real-time:** Update technical documentation as changes occur
- **Weekly:** Review and update migration status
- **Monthly:** Publish lessons learned and best practices
- **End of Phase:** Complete documentation review and update

---

## Next Steps (Immediate Actions)

### For Current ETL Work:
1. ✅ **Complete `securitylog` pipeline** with existing conventions
2. ✅ **Document current performance baseline**
3. ✅ **Validate end-to-end data flow**
4. ✅ **Create stable release of current system**

### For Implementation Preparation:
1. 📋 **Review and finalize** `Improved_naming_conventions.md`
2. 🛠️ **Begin macro development** for common patterns
3. 📚 **Create detailed migration playbook**
4. 🎓 **Plan team training schedule**

---

## Conclusion

This phased approach balances the need for improved conventions with system stability and team productivity. By establishing a solid ETL baseline first, we create a stable foundation for systematic improvement.

The strategy prioritizes:
- **Stability** over speed
- **Validation** over assumption
- **Team adoption** over technical perfection
- **Business continuity** over implementation purity

**Next Review:** After ETL baseline establishment  
**Owner:** Technical Lead  
**Stakeholders:** Development Team, Data Analytics Team, Business Users

---

*Document maintained by: Technical Team*  
*Last updated: December 6, 2025* 