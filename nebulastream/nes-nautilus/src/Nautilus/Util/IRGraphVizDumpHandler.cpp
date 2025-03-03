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

#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/Util/IRDumpHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>

namespace NES::Nautilus::IR {

NESIRDumpHandler::NESIRDumpHandler(std::ostream& out) : out(out) {}

std::shared_ptr<NESIRDumpHandler> NESIRDumpHandler::create(std::ostream& out) { return std::make_shared<NESIRDumpHandler>(out); }

IR::BasicBlockPtr NESIRDumpHandler::getNextLowerOrEqualLevelBasicBlock(BasicBlockPtr thenBlock, int ifParentBlockLevel) {
    auto terminatorOp = thenBlock->getOperations().back();
    if (terminatorOp->getOperationType() == Operations::Operation::BranchOp) {
        auto branchOp = std::static_pointer_cast<Operations::BranchOperation>(terminatorOp);
        if (branchOp->getNextBlockInvocation().getBlock()->getScopeLevel() <= (uint32_t) ifParentBlockLevel) {
            return branchOp->getNextBlockInvocation().getBlock();
        } else {
            return getNextLowerOrEqualLevelBasicBlock(branchOp->getNextBlockInvocation().getBlock(), ifParentBlockLevel);
        }
    } else if (terminatorOp->getOperationType() == Operations::Operation::LoopOp) {
        auto loopOp = std::static_pointer_cast<Operations::LoopOperation>(terminatorOp);
        auto loopIfOp =
            std::static_pointer_cast<Operations::IfOperation>(loopOp->getLoopHeadBlock().getBlock()->getOperations().back());
        if (loopIfOp->getTrueBlockInvocation().getBlock()->getScopeLevel() <= (uint32_t) ifParentBlockLevel) {
            return loopIfOp->getTrueBlockInvocation().getBlock();
        } else {
            return getNextLowerOrEqualLevelBasicBlock(loopIfOp->getFalseBlockInvocation().getBlock(), ifParentBlockLevel);
        }
    } else if (terminatorOp->getOperationType() == Operations::Operation::IfOp) {
        auto ifOp = std::static_pointer_cast<Operations::IfOperation>(terminatorOp);
        if (ifOp->getFalseBlockInvocation().getBlock() != nullptr) {
            return getNextLowerOrEqualLevelBasicBlock(ifOp->getFalseBlockInvocation().getBlock(), ifParentBlockLevel);
        } else {
            return getNextLowerOrEqualLevelBasicBlock(ifOp->getTrueBlockInvocation().getBlock(), ifParentBlockLevel);
        }
    } else {//ReturnOp todo changed #3234
        return nullptr;
    }
}

void NESIRDumpHandler::dumpHelper(OperationPtr const& terminatorOp, int32_t scopeLevel) {
    switch (terminatorOp->getOperationType()) {
        case Operations::Operation::OperationType::BranchOp: {
            auto branchOp = std::static_pointer_cast<Operations::BranchOperation>(terminatorOp);
            //if (branchOp->getNextBlockInvocation().getBlock()->getScopeLevel() > scopeLevel) {
            dumpHelper(branchOp->getNextBlockInvocation().getBlock());
            //}
            break;
        }
        case Operations::Operation::OperationType::LoopOp: {
            auto loopOperation = std::static_pointer_cast<Operations::LoopOperation>(terminatorOp);
            dumpHelper(loopOperation->getLoopHeadBlock().getBlock());
            break;
        }
        case Operations::Operation::OperationType::IfOp: {
            auto ifOp = std::static_pointer_cast<Operations::IfOperation>(terminatorOp);
            BasicBlockPtr lastTerminatorOp = getNextLowerOrEqualLevelBasicBlock(
                ifOp->getTrueBlockInvocation().getBlock(),
                ifOp->getTrueBlockInvocation().getBlock()->getScopeLevel() - 1);//todo can lead to error #3234
            dumpHelper(ifOp->getTrueBlockInvocation().getBlock());
            if (ifOp->getFalseBlockInvocation().getBlock()
                && ifOp->getFalseBlockInvocation().getBlock()->getScopeLevel()
                    >= (uint32_t) scopeLevel) {//todo remove scopeLevel check #3234
                dumpHelper(ifOp->getFalseBlockInvocation().getBlock());
            }
            if (lastTerminatorOp) {
                dumpHelper(lastTerminatorOp);
            }
            break;
        }
        case Operations::Operation::OperationType::ReturnOp: break;
        default: break;
    }
}

void NESIRDumpHandler::dumpHelper(BasicBlockPtr const& basicBlock) {
    if (!visitedBlocks.contains(basicBlock->getIdentifier())) {
        int32_t indent = basicBlock->getScopeLevel() + 1;
        visitedBlocks.emplace(basicBlock->getIdentifier());
        //out << '\n' << std::string(basicBlock->getScopeLevel() * 4, ' ') << "Block_" << basicBlock->getIdentifier() << '(';
        out << '\n' << "Block_" << basicBlock->getIdentifier() << '(';
        if (basicBlock->getArguments().size() > 0) {
            out << basicBlock->getArguments().at(0)->getIdentifier() + ":"
                << basicBlock->getArguments().at(0)->getStamp()->toString();
            for (int i = 1; i < (int) basicBlock->getArguments().size(); ++i) {
                out << ", " << basicBlock->getArguments().at(i)->getIdentifier() << ":"
                    << basicBlock->getArguments().at(0)->getStamp()->toString();
            }
        }
        out << "):" << '\n';
        for (auto operation : basicBlock->getOperations()) {
            out << std::string(4, ' ') << operation->toString() << " :" << operation->getStamp()->toString() << std::endl;
            //  out << std::string(indent * 4, ' ') << operation->toString() << std::endl;
        }
        OperationPtr terminatorOp = basicBlock->getOperations().back();
        dumpHelper(terminatorOp, basicBlock->getScopeLevel());
    }
}

void NESIRDumpHandler::dump(const std::shared_ptr<Operations::FunctionOperation> funcOp) {
    out << funcOp->toString() << " {";
    dumpHelper(funcOp->getFunctionBasicBlock());
    out << "}\n";
}

}// namespace NES::Nautilus::IR
