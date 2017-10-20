import java.io.File
import java.util.concurrent.LinkedBlockingQueue

//
data class Account(val name: String, var balance: Long)

data class Transaction(val from: String, val to: String, val amount: Long, val submitTime: Long)

fun main(args: Array<String>) {

    val level = "level1"

    File("input").listFiles({ dir, filename -> filename.startsWith(level) }).forEach {
        processFile(it)
    }
}

fun processFile(file: File) {
    val lines = LinkedBlockingQueue(file.readLines())

    val numberOfAccounts = lines.poll().toInt()
    val accounts = (1..numberOfAccounts).map {
        val parts = lines.poll().split(" ")
        Account(parts[0], parts[1].toLong())
    }

    val numerOfTransactions = lines.poll().toInt()
    val transactions = (1..numerOfTransactions).map {
        val parts = lines.poll().split(" ")
        Transaction(parts[0], parts[1], parts[2].toLong(), parts[3].toLong())
    }

    transactions.forEach {
        processTransaction(it, accounts)
    }


    val result = accounts.map { "${it.name} ${it.balance}" }.joinToString("\n")

    File("output/" + file.name).apply {
        delete()
        createNewFile()
        appendText(accounts.count().toString())
        appendText("\n")
        appendText(result)
    }
}

fun processTransaction(transaction: Transaction, accounts: List<Account>) {

    val from = accounts.single { it.name == transaction.from }
    val to = accounts.single { it.name == transaction.to }

    from.balance -= transaction.amount
    to.balance += transaction.amount
}
