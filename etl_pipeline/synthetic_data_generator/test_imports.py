"""
Test Import Script
==================

Quick test to verify all generator modules can be imported successfully.
Run this before attempting to generate data to catch any syntax/import errors.

Usage:
    python test_imports.py
"""

def test_imports():
    """Test that all generators can be imported"""
    print("=" * 60)
    print("Testing Generator Imports...")
    print("=" * 60)
    
    try:
        print("\n1. Testing main.py imports...")
        from main import SyntheticDataGenerator, GeneratorConfig
        print("   ✅ main.py imports successfully")
        
        print("\n2. Testing foundation generator...")
        from generators.foundation_data_generator import FoundationGenerator
        print("   ✅ foundation_data_generator.py imports successfully")
        
        print("\n3. Testing patient generator...")
        from generators.patient_generator import PatientGenerator
        print("   ✅ patient_generator.py imports successfully")
        
        print("\n4. Testing insurance generator...")
        from generators.insurance_generator import InsuranceGenerator
        print("   ✅ insurance_generator.py imports successfully")
        
        print("\n5. Testing clinical generator...")
        from generators.clinical_generator import ClinicalGenerator
        print("   ✅ clinical_generator.py imports successfully")
        
        print("\n6. Testing financial generator...")
        from generators.financial_generator import FinancialGenerator
        print("   ✅ financial_generator.py imports successfully")
        
        print("\n7. Testing supporting generator...")
        from generators.supporting_generator import SupportingGenerator
        print("   ✅ supporting_generator.py imports successfully")
        
        print("\n8. Testing generators package...")
        import generators
        print("   ✅ generators package imports successfully")
        
        print("\n" + "=" * 60)
        print("✅ ALL IMPORTS SUCCESSFUL!")
        print("=" * 60)
        print("\nYou can now run: python main.py --patients 100")
        return True
        
    except ImportError as e:
        print(f"\n❌ IMPORT ERROR: {e}")
        print("\nPlease check:")
        print("  1. Are you in the correct directory? (etl_pipeline/synthetic_data_generator)")
        print("  2. Is your virtual environment activated?")
        print("  3. Did you install requirements? (pip install -r requirements.txt)")
        return False
    
    except SyntaxError as e:
        print(f"\n❌ SYNTAX ERROR: {e}")
        print("\nThere's a syntax error in one of the generator files.")
        print("Please review the error message above for details.")
        return False
    
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        print("\nAn unexpected error occurred. Please review the traceback above.")
        return False


if __name__ == '__main__':
    success = test_imports()
    exit(0 if success else 1)

