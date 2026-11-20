//===----------------------------------------------------------------------===//
//
// This source file is part of the AsyncHTTPClient open source project
//
// Copyright (c) 2026 Apple Inc. and the AsyncHTTPClient project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of AsyncHTTPClient project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIOEmbedded
import Testing

@testable import AsyncHTTPClient

@Suite struct TransactionCancelHandlerTests {
    typealias StateMachine = TransactionCancelHandler.StateMachine
    typealias CancelReason = TransactionCancelHandler.CancelReason

    @Test func registerWithoutCancellationProceeds() async throws {
        try await Transaction.withTestTransaction { transaction, _ in
            var stateMachine = StateMachine()

            let action = stateMachine.register(transaction)

            guard case .proceed = action else {
                Issue.record("unexpected action \(action)")
                return
            }
        }
    }

    @Test(arguments: [CancelReason.taskCanceled, .deadlineExceeded])
    func cancelBeforeRegisterCancelsTransactionOnRegister(reason: CancelReason) async throws {
        try await Transaction.withTestTransaction { transaction, _ in
            var stateMachine = StateMachine()

            let cancelAction = stateMachine.cancel(reason: reason)
            let registerAction = stateMachine.register(transaction)

            guard case .none = cancelAction else {
                Issue.record("unexpected action \(cancelAction)")
                return
            }
            guard case .cancel(let cancelledTransaction, let cancelReason) = registerAction else {
                Issue.record("unexpected action \(registerAction)")
                return
            }
            #expect(cancelledTransaction === transaction)
            #expect(cancelReason == reason)
        }
    }

    @Test(arguments: [CancelReason.taskCanceled, .deadlineExceeded])
    func cancelAfterRegisterCancelsTransaction(reason: CancelReason) async throws {
        try await Transaction.withTestTransaction { transaction, _ in
            var stateMachine = StateMachine()
            _ = stateMachine.register(transaction)

            let action = stateMachine.cancel(reason: reason)

            guard case .cancel(let cancelledTransaction, let cancelReason) = action else {
                Issue.record("unexpected action \(action)")
                return
            }
            #expect(cancelledTransaction === transaction)
            #expect(cancelReason == reason)
        }
    }

    @Test func secondCancelBeforeRegisterKeepsFirstReason() async throws {
        try await Transaction.withTestTransaction { transaction, _ in
            var stateMachine = StateMachine()
            _ = stateMachine.cancel(reason: .deadlineExceeded)

            let secondCancelAction = stateMachine.cancel(reason: .taskCanceled)
            let registerAction = stateMachine.register(transaction)

            guard case .none = secondCancelAction else {
                Issue.record("unexpected action \(secondCancelAction)")
                return
            }
            guard case .cancel(_, let cancelReason) = registerAction else {
                Issue.record("unexpected action \(registerAction)")
                return
            }
            #expect(cancelReason == .deadlineExceeded)
        }
    }

    @Test func secondCancelAfterRegisterIsIgnored() async throws {
        try await Transaction.withTestTransaction { transaction, _ in
            var stateMachine = StateMachine()
            _ = stateMachine.register(transaction)
            _ = stateMachine.cancel(reason: .taskCanceled)

            let action = stateMachine.cancel(reason: .deadlineExceeded)

            guard case .none = action else {
                Issue.record("unexpected action \(action)")
                return
            }
        }
    }

    @Test func handlerRegisterAfterCancelFailsTransactionWithoutExecuting() async throws {
        try await Transaction.withTestTransaction { transaction, responseTask in
            let handler = TransactionCancelHandler()
            handler.cancel(reason: .taskCanceled)

            let shouldExecute = handler.register(transaction)

            #expect(!shouldExecute)
            await #expect(throws: CancellationError.self) { try await responseTask.value }
        }
    }

    @Test func handlerDeadlineAfterRegisterFailsTransaction() async throws {
        try await Transaction.withTestTransaction { transaction, responseTask in
            let handler = TransactionCancelHandler()
            let shouldExecute = handler.register(transaction)

            handler.cancel(reason: .deadlineExceeded)

            #expect(shouldExecute)
            await #expect(throws: HTTPClientError.deadlineExceeded) { try await responseTask.value }
        }
    }
}

extension Transaction {
    fileprivate static func withTestTransaction(
        _ body: (Transaction, Task<HTTPClientResponse, any Error>) async -> Void
    ) async throws {
        let eventLoop = NIOAsyncTestingEventLoop()
        let request = try PreparedRequest(HTTPClientRequest(url: "https://localhost/"))
        let (transaction, responseTask) = await Transaction.makeWithResultTask(
            request: request,
            preferredEventLoop: eventLoop
        )

        await body(transaction, responseTask)

        // The transaction owns the response continuation, which must be resumed before the test ends.
        transaction.cancel()
        _ = await responseTask.result
        await eventLoop.shutdownGracefully()
    }
}
