/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <fmt/core.h>
#include <random>

namespace NES {
class SchemaTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SchemaTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("SchemaTest test class SetUpTestCase.");
    }
    static void TearDownTestCase() { NES_INFO("SchemaTest test class TearDownTestCase."); }

    auto getRandomFields(const auto numberOfFields) {
        auto getRandomBasicType = [](const unsigned long rndPos) {
            const auto& values = magic_enum::enum_values<BasicType>();
            std::uniform_int_distribution<unsigned long>(0, values.size() - 1);
            return values[rndPos % values.size()];
        };
        constexpr auto RND_SEED = 42;
        std::random_device rd;
        std::mt19937 mt(RND_SEED);

        std::vector<AttributeFieldPtr> rndFields;
        for (auto fieldCnt = 0_u64; fieldCnt < numberOfFields; ++fieldCnt) {
            const auto fieldName = fmt::format("field{}", fieldCnt);
            const auto basicType = getRandomBasicType(mt());
            rndFields.emplace_back(AttributeField::create(fieldName, DataTypeFactory::createType(basicType)));
        }

        return rndFields;
    }
};

TEST_F(SchemaTest, createTest) {
    {
        // Checking for default values
        SchemaPtr testSchema;
        ASSERT_NO_THROW(testSchema = Schema::create());
        ASSERT_TRUE(testSchema);
        ASSERT_EQ(testSchema->getLayoutType(), Schema::MemoryLayoutType::ROW_LAYOUT);
    }

    {
        // Checking with row memory layout
        SchemaPtr testSchema;
        ASSERT_NO_THROW(testSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT));
        ASSERT_TRUE(testSchema);
        ASSERT_EQ(testSchema->getLayoutType(), Schema::MemoryLayoutType::ROW_LAYOUT);
    }

    {
        // Checking with col memory layout
        SchemaPtr testSchema;
        ASSERT_NO_THROW(testSchema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT));
        ASSERT_TRUE(testSchema);
        ASSERT_EQ(testSchema->getLayoutType(), Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
    }
}

TEST_F(SchemaTest, addFieldTest) {
    {
        // Adding one field
        for (const auto& basicTypeVal : magic_enum::enum_values<BasicType>()) {
            SchemaPtr testSchema;
            ASSERT_NO_THROW(testSchema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT));
            ASSERT_EQ(testSchema->getLayoutType(), Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
            ASSERT_TRUE(testSchema->addField("field", basicTypeVal));
            ASSERT_EQ(testSchema->fields.size(), 1);
            ASSERT_EQ(testSchema->fields[0]->getName(), "field");
            ASSERT_TRUE(testSchema->fields[0]->getDataType()->equals(DataTypeFactory::createType(basicTypeVal)));
        }
    }

    {
        // Adding multiple fields
        constexpr auto NUM_FIELDS = 10_u64;
        auto rndFields = getRandomFields(NUM_FIELDS);
        auto testSchema = Schema::create();
        for (const auto& field : rndFields) {
            ASSERT_TRUE(testSchema->addField(field));
        }

        ASSERT_EQ(testSchema->fields.size(), rndFields.size());
        for (auto fieldCnt = 0_u64; fieldCnt < rndFields.size(); ++fieldCnt) {
            const auto& curField = rndFields[fieldCnt];
            EXPECT_TRUE(testSchema->fields[fieldCnt]->isEqual(curField));
            EXPECT_TRUE(testSchema->get(fieldCnt)->isEqual(curField));
            EXPECT_TRUE(testSchema->get(curField->getName())->isEqual(curField));
        }
    }
}

TEST_F(SchemaTest, removeFieldsTest) {
    constexpr auto NUM_FIELDS = 10_u64;
    auto rndFields = getRandomFields(NUM_FIELDS);
    auto testSchema = Schema::create();
    for (const auto& field : rndFields) {
        ASSERT_TRUE(testSchema->addField(field));
    }

    ASSERT_EQ(testSchema->fields.size(), rndFields.size());
    for (auto fieldCnt = 0_u64; fieldCnt < rndFields.size(); ++fieldCnt) {
        const auto& curField = rndFields[fieldCnt];
        EXPECT_TRUE(testSchema->fields[fieldCnt]->isEqual(curField));
        EXPECT_TRUE(testSchema->get(fieldCnt)->isEqual(curField));
        EXPECT_TRUE(testSchema->get(curField->getName())->isEqual(curField));
    }

    // Removing fields while we still have one field
    while (testSchema->getSize() > 0) {
        const auto rndPos = rand() % testSchema->getSize();
        const auto& fieldToRemove = rndFields[rndPos];
        EXPECT_NO_THROW(testSchema->removeField(fieldToRemove));
        EXPECT_ANY_THROW(testSchema->get(fieldToRemove->getName()));

        rndFields.erase(rndFields.begin() + rndPos);
    }
}

TEST_F(SchemaTest, replaceFieldTest) {
    {
        // Replacing one field with a random one
        for (const auto& basicTypeVal : magic_enum::enum_values<BasicType>()) {
            SchemaPtr testSchema;
            ASSERT_NO_THROW(testSchema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT));
            ASSERT_EQ(testSchema->getLayoutType(), Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
            ASSERT_TRUE(testSchema->addField("field", basicTypeVal));
            ASSERT_EQ(testSchema->fields.size(), 1);
            ASSERT_EQ(testSchema->fields[0]->getName(), "field");
            ASSERT_TRUE(testSchema->fields[0]->getDataType()->equals(DataTypeFactory::createType(basicTypeVal)));

            // Replacing field
            const auto newDataType = getRandomFields(1_u64)[0]->getDataType();
            ASSERT_NO_THROW(testSchema->replaceField("field", newDataType));
            ASSERT_TRUE(testSchema->fields[0]->getDataType()->equals(newDataType));
        }
    }

    {
        // Adding multiple fields
        constexpr auto NUM_FIELDS = 10_u64;
        auto rndFields = getRandomFields(NUM_FIELDS);
        auto testSchema = Schema::create();
        for (const auto& field : rndFields) {
            ASSERT_TRUE(testSchema->addField(field));
        }

        ASSERT_EQ(testSchema->fields.size(), rndFields.size());
        for (auto fieldCnt = 0_u64; fieldCnt < rndFields.size(); ++fieldCnt) {
            const auto& curField = rndFields[fieldCnt];
            EXPECT_TRUE(testSchema->fields[fieldCnt]->isEqual(curField));
            EXPECT_TRUE(testSchema->get(fieldCnt)->isEqual(curField));
            EXPECT_TRUE(testSchema->get(curField->getName())->isEqual(curField));
        }

        // Replacing multiple fields with new data types
        auto replacingFields = getRandomFields(NUM_FIELDS);
        for (const auto& replaceField : replacingFields) {
            testSchema->replaceField(replaceField->getName(), replaceField->getDataType());
        }

        for (auto fieldCnt = 0_u64; fieldCnt < replacingFields.size(); ++fieldCnt) {
            const auto& curField = replacingFields[fieldCnt];
            EXPECT_TRUE(testSchema->fields[fieldCnt]->isEqual(curField));
            EXPECT_TRUE(testSchema->get(fieldCnt)->isEqual(curField));
            EXPECT_TRUE(testSchema->get(curField->getName())->isEqual(curField));
        }
    }
}

TEST_F(SchemaTest, getSchemaSizeInBytesTest) {
    {
        // Calculating the schema size for each data type
        DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
        for (const auto& basicTypeVal : magic_enum::enum_values<BasicType>()) {
            SchemaPtr testSchema;
            ASSERT_NO_THROW(testSchema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT));
            ASSERT_EQ(testSchema->getLayoutType(), Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
            ASSERT_TRUE(testSchema->addField("field", basicTypeVal));
            ASSERT_EQ(testSchema->fields.size(), 1);
            ASSERT_EQ(testSchema->fields[0]->getName(), "field");
            ASSERT_TRUE(testSchema->fields[0]->getDataType()->equals(DataTypeFactory::createType(basicTypeVal)));
            ASSERT_EQ(testSchema->getSchemaSizeInBytes(),
                      defaultPhysicalTypeFactory.getPhysicalType(DataTypeFactory::createType(basicTypeVal))->size());
        }
    }

    {
        using enum NES::BasicType;
        // Calculating the schema size for multiple fields
        auto testSchema = Schema::create()
                              ->addField("field1", UINT8)
                              ->addField("field2", UINT16)
                              ->addField("field3", INT32)
                              ->addField("field4", FLOAT32)
                              ->addField("field5", FLOAT64);
        EXPECT_EQ(testSchema->getSchemaSizeInBytes(), 1 + 2 + 4 + 4 + 8);
    }
}

TEST_F(SchemaTest, containsTest) {
    using enum NES::BasicType;
    {
        // Checking contains for one fieldName
        auto testSchema = Schema::create()->addField("field1", UINT8);
        EXPECT_TRUE(testSchema->contains("field1"));
        EXPECT_FALSE(testSchema->contains("notExistingField1"));
    }

    {
        // Checking contains with multiple fields
        auto testSchema = Schema::create()
                              ->addField("field1", UINT8)
                              ->addField("field2", UINT16)
                              ->addField("field3", INT32)
                              ->addField("field4", FLOAT32)
                              ->addField("field5", FLOAT64);

        // Existing fields
        EXPECT_TRUE(testSchema->contains("field3"));

        // Not existing fields
        EXPECT_FALSE(testSchema->contains("notExistingField3"));
    }
}

TEST_F(SchemaTest, getSourceNameQualifierTest) {
    using enum NES::BasicType;
    // TODO once #4355 is done, we can use updateSourceName(source1) here
    const auto sourceName = std::string("source1");
    auto testSchema = Schema::create()
                          ->addField(sourceName + "$field1", UINT8)
                          ->addField(sourceName + "$field2", UINT16)
                          ->addField(sourceName + "$field3", INT32)
                          ->addField(sourceName + "$field4", FLOAT32)
                          ->addField(sourceName + "$field5", FLOAT64);

    EXPECT_EQ(testSchema->getSourceNameQualifier(), sourceName);
}

TEST_F(SchemaTest, copyTest) {
    auto testSchema = Schema::create()->addField("field1", BasicType::UINT8)->addField("field2", BasicType::UINT16);
    auto testSchemaCopy = testSchema->copy();

    ASSERT_EQ(testSchema->getSchemaSizeInBytes(), testSchemaCopy->getSchemaSizeInBytes());
    ASSERT_EQ(testSchema->getLayoutType(), testSchemaCopy->getLayoutType());
    ASSERT_EQ(testSchema->fields.size(), testSchemaCopy->fields.size());

    // Comparing fields
    for (auto fieldCnt = 0_u64; fieldCnt < testSchemaCopy->fields.size(); ++fieldCnt) {
        const auto& curField = testSchemaCopy->get(fieldCnt);
        EXPECT_TRUE(testSchema->fields[fieldCnt]->isEqual(curField));
        EXPECT_TRUE(testSchema->get(fieldCnt)->isEqual(curField));
        EXPECT_TRUE(testSchema->get(curField->getName())->isEqual(curField));
    }
}

TEST_F(SchemaTest, updateSourceNameTest) {
    // TODO once #4355 is done, we can test updateSourceName(source1) here
}

}// namespace NES
