/*
 * Copyright (c) 2016 Razeware LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.raywenderlich.cheesefinder;

import android.text.Editable;
import android.text.TextWatcher;
import android.view.View;

import java.util.List;
import java.util.concurrent.TimeUnit;

import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.ObservableOnSubscribe;
import io.reactivex.android.schedulers.AndroidSchedulers;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Cancellable;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import io.reactivex.schedulers.Schedulers;

public class CheeseActivity extends BaseSearchActivity {

    private Disposable mDisposable;

    @Override
    protected void onStart() {
        super.onStart();

        Observable<String> buttonClickStream = createButtonClickObservable();
        Observable<String> textChangeStream = createTextChangeObservable();

        Observable<String> searchTextObservable = Observable.merge(textChangeStream, buttonClickStream);

        mDisposable = searchTextObservable
                .observeOn(AndroidSchedulers.mainThread())
                .doOnNext(s -> showProgressBar())
                .observeOn(Schedulers.io())
                .map(query -> mCheeseSearchEngine.search(query))
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(result -> {
                    hideProgressBar();
                    showResult(result);
                });
    }

    private Observable<String> createTextChangeObservable() {
        Observable<String> textChangeObservable = Observable.create(emitter -> {
            final TextWatcher watcher = new TextWatcher() {
                @Override
                public void beforeTextChanged(CharSequence s, int start, int count, int after) {}

                @Override
                public void afterTextChanged(Editable s) {}

                @Override
                public void onTextChanged(CharSequence s, int start, int before, int count) {
                    emitter.onNext(s.toString());
                }
            };

            mQueryEditText.addTextChangedListener(watcher);

            emitter.setCancellable(() -> mQueryEditText.removeTextChangedListener(watcher));
        });
        return textChangeObservable
                .filter(query -> query.length() >= 2).debounce(1000, TimeUnit.MILLISECONDS);
    }


    private Observable<String> createButtonClickObservable() {
        return Observable.create(emitter -> {
            mSearchButton.setOnClickListener(view -> emitter.onNext(mQueryEditText.getText().toString()));

            emitter.setCancellable(() -> mSearchButton.setOnClickListener(null));
        });
    }

    @Override
    protected void onStop() {
        super.onStop();
        if (!mDisposable.isDisposed()) {
            mDisposable.dispose();
        }
    }
}
